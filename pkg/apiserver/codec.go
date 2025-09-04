package apiserver

import (
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

var _ runtime.Serializer = &CompositeCodec{}
var _ runtime.NegotiatedSerializer = &CompositeCodecFactory{}

// CompositeCodec embeds a runtime.Codec and overrides Encode for view objects.
type CompositeCodec struct {
	runtime.Codec                 // embed the default codec
	encoder       runtime.Encoder // an encoder we will override
}

// NewCompositeCodec creates a new composite codec.
func NewCompositeCodec(defaultCodec runtime.Codec, encoder runtime.Encoder) *CompositeCodec {
	return &CompositeCodec{
		Codec:   defaultCodec,
		encoder: encoder,
	}
}

// Encode overrides the embedded codec's Encode method.
func (c *CompositeCodec) Encode(obj runtime.Object, w io.Writer) error {
	// Check if object is a View or a ViewList
	if !viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		// Not a view object, use the embedded codec's default behavior
		return c.Codec.Encode(obj, w)
	}

	// Do not do any conversion, encode unstructured object as is.
	return c.encoder.Encode(obj, w)
}

// CompositeCodecFactory implements a factory that creates composite codecs.
type CompositeCodecFactory struct {
	defaultFactory serializer.CodecFactory
	scheme         *runtime.Scheme
}

// NewCompositeCodecFactory creates a new composite codec factory.
func NewCompositeCodecFactory(defaultFactory serializer.CodecFactory, scheme *runtime.Scheme) *CompositeCodecFactory {
	return &CompositeCodecFactory{
		defaultFactory: defaultFactory,
		scheme:         scheme,
	}
}

// CodecForVersions implements serializer.CodecFactory.
func (f *CompositeCodecFactory) CodecForVersions(encoder runtime.Encoder, decoder runtime.Decoder, encode runtime.GroupVersioner, decode runtime.GroupVersioner) runtime.Codec {
	defaultCodec := f.defaultFactory.CodecForVersions(encoder, decoder, encode, decode)
	return NewCompositeCodec(defaultCodec, encoder)
}

// EncoderForVersion returns an encoder that ensures objects being written to the provided
// serializer are in the provided group version.
func (f *CompositeCodecFactory) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return f.CodecForVersions(encoder, nil, gv, nil)
}

// DecoderToVersion returns a decoder that ensures objects being read by the provided
// serializer are in the provided group version by default.
func (f *CompositeCodecFactory) DecoderToVersion(decoder runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	return f.CodecForVersions(nil, decoder, nil, gv)
}

// SupportedMediaTypes is the media types supported for reading and writing single objects.
func (f *CompositeCodecFactory) SupportedMediaTypes() []runtime.SerializerInfo {
	return f.defaultFactory.SupportedMediaTypes()
}
