package pipeline

import (
	"fmt"
	"reflect"

	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/dbsp"
	"github.com/l7mp/dcontroller/pkg/object"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (p *Pipeline) ConvertDeltaToZSet(delta cache.Delta) (*dbsp.DocumentZSet, error) {
	gvk := delta.Object.GetObjectKind().GroupVersionKind()
	if _, ok := p.sourceCache[gvk]; !ok {
		p.sourceCache[gvk] = cache.NewStore()
	}

	var old object.Object
	if obj, exists, err := p.sourceCache[gvk].Get(delta.Object); err == nil && exists {
		old = obj
	}

	zset := dbsp.NewDocumentZSet()
	switch delta.Type {
	case cache.Added:
		if err := zset.AddDocumentMutate(delta.Object.UnstructuredContent(), 1); err != nil {
			return nil, NewPipelineError(
				fmt.Errorf("processing event %q: could not add object %s to zset: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := p.sourceCache[gvk].Add(delta.Object); err != nil {
			return nil, fmt.Errorf("processing event %q: could not add object %s to store: %w",
				delta.Type, ObjectKey(delta.Object), err)
		}

	case cache.Updated, cache.Replaced, cache.Upserted:
		// delete followed by an add
		if old != nil {
			if err := zset.AddDocumentMutate(old.UnstructuredContent(), -1); err != nil {
				return nil, NewPipelineError(
					fmt.Errorf("processing event %q: could not add object %s to zset: %w",
						delta.Type, ObjectKey(delta.Object), err))
			}
		}

		if err := zset.AddDocumentMutate(delta.Object.UnstructuredContent(), 1); err != nil {
			return nil, NewPipelineError(
				fmt.Errorf("processing event %q: could not add object %s to zset: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := p.sourceCache[gvk].Update(delta.Object); err != nil {
			return nil, fmt.Errorf("processing event %q: could not add object %s to store: %w",
				delta.Type, ObjectKey(delta.Object), err)
		}

	case cache.Deleted:
		if old == nil {
			return nil, NewPipelineError(
				fmt.Errorf("processing event %q: got delete for a nonexistent object %s",
					delta.Type, ObjectKey(delta.Object)))
		}

		if err := zset.AddDocumentMutate(delta.Object.UnstructuredContent(), -1); err != nil {
			return nil, NewPipelineError(
				fmt.Errorf("processing event %q: could not add object %s to zset: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := p.sourceCache[gvk].Delete(old); err != nil {
			return nil, fmt.Errorf("processing event %q: could not delete object %s from store: %w",
				delta.Type, ObjectKey(delta.Object), err)
		}

	default:
		return nil, NewPipelineError(
			fmt.Errorf("unknown event %q for object %s", delta.Type, ObjectKey(delta.Object)))
	}

	return zset, nil
}

func (p *Pipeline) ConvertZSetToDelta(zset *dbsp.DocumentZSet, view string) ([]cache.Delta, error) {
	ds := []cache.Delta{}

	docEntries, err := zset.List()
	if err != nil {
		return nil, err
	}

	// for _, entry := range collapseDeltas(docEntries) {
	for _, entry := range docEntries {
		doc := entry.Document
		var deltaType cache.DeltaType
		switch {
		case entry.Multiplicity > 0:
			deltaType = cache.Added // collapse multi-objects into a single instance
		case entry.Multiplicity < 0:
			deltaType = cache.Deleted
		default:
			continue // ignore
		}

		// metadata: must exist
		meta, ok := doc["metadata"]
		if !ok {
			return nil, NewInvalidObjectError("no metadata in object")
		}
		metaMap, ok := meta.(dbsp.Document)
		if !ok {
			return nil, NewInvalidObjectError("invalid metadata in object")
		}

		// namespace: can be empty
		namespaceStr := ""
		namespace, ok := metaMap["namespace"]
		if ok {
			if reflect.ValueOf(namespace).Kind() != reflect.String {
				return nil, NewInvalidObjectError(fmt.Sprintf("metadata/namespace must be "+
					"a string (current value %q)", namespace))
			}
			namespaceStr = namespace.(string)
			metaMap["namespace"] = namespaceStr
		}

		// name must be defined
		name, ok := metaMap["name"]
		if !ok {
			return nil, NewInvalidObjectError("missing metadata/name")
		}
		if reflect.ValueOf(name).Kind() != reflect.String {
			return nil, NewInvalidObjectError(fmt.Sprintf("metadata/name must be a string "+
				"(current value %q)", name))
		}
		nameStr := name.(string)
		if nameStr == "" {
			return nil, NewInvalidObjectError("empty metadata/name in aggregation result")
		}
		metaMap["name"] = nameStr

		// encapsulate in an object
		obj := object.NewViewObject(view)
		object.SetContent(obj, doc)
		// still needed
		obj.SetName(nameStr)
		obj.SetNamespace(namespaceStr)

		ds = append(ds, cache.Delta{Object: obj, Type: deltaType})
	}

	return ds, nil
}

// Reconcile processes a delta set containing only unrdered(!) add/delete ops into a proper
// ordered(!) upsert/delete delta list.
//
// DBSP outputs onordered zsets so there is no way to know for documents that map to the same
// primary key whether an add or a delete comes first, and the two orders yield different
// results. To remove this ambiguity, we maintain a target cache that contains the latest known
// state of the target view and we take the (doc->+/-1) pairs in any order from the zset result
// set. The rules are as follows:
//
// - for additions (doc->+1), we extract the primary key from doc and immediately upsert doc into
// the cache with that key and add the upsert delta to our result set, possibly overwriting any
// previous delta for the same key
//
// - for deletions (doc->-1), we again extract the primary key from doc and first we fetch the
// current entry doc' from the cache and check if doc==doc'. If there is no entry in the cache for
// the key or the latest state equals the doc to be deleted, we add the delete to the cache and the
// result delta, otherwise we drop the delete event and move on.
func (p *Pipeline) Reconcile(ds []cache.Delta) ([]cache.Delta, error) {
	deltaCache := map[string]cache.Delta{}

	for _, d := range ds {
		key := client.ObjectKeyFromObject(d.Object).String()

		switch d.Type {
		case cache.Added:
			// Addition: Always upsert, may overwrite previous delta
			if err := p.targetCache.Add(d.Object); err != nil {
				return nil, err
			}

			d.Type = cache.Upserted
			deltaCache[key] = d

		case cache.Deleted:
			// Deletion: Delete, but only if there is no entry in the target cache for
			// that object or the previous entry was for the exact same document
			obj, exists, err := p.targetCache.Get(d.Object)
			if err != nil {
				return nil, err
			}

			same := false
			if exists {
				eq, err := dbsp.DeepEqual(obj.UnstructuredContent(), d.Object.UnstructuredContent())
				if err != nil {
					return nil, err
				}
				same = eq
			}

			if !exists || same {
				d.Type = cache.Deleted
				deltaCache[key] = d
			}

		default:
			return nil, fmt.Errorf("unknown delta in zset: %s", d.Type)
		}
	}

	// convert delta cache back to delta: first the delete ops, then the upserts
	res := []cache.Delta{}
	for _, d := range deltaCache {
		if d.Type == cache.Deleted {
			res = append(res, d)
		}
	}
	for _, d := range deltaCache {
		if d.Type == cache.Upserted {
			res = append(res, d)
		}
	}

	return res, nil
}
