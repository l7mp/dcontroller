package pipeline

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("SQL Examples", func() {
	// https://www.feldera.com/blog/backfill-explained
	Context("should evaluate a complex aggregation", func() {
		var id = 0
		var gradeP, statP Evaluator

		var makeGrade = func(name, class string, grade int) object.Object {
			elem := object.NewViewObject("test", "grade")
			object.SetContent(elem, map[string]any{
				"name":  name,
				"class": class,
				"grade": int64(grade),
			})
			object.SetName(elem, "default", fmt.Sprintf("%s-%d", name, id))
			id++
			return elem
		}
		var checkGrade = func(ds []object.Delta, name, class string, grade int, avg float64) {
			GinkgoHelper()
			for _, d := range ds {
				res := d.Object.UnstructuredContent()
				if res["name"] == name && res["class"] == class {
					Expect(res["grade"]).To(Equal(int64(grade)))
					Expect(res["avg"]).To(Equal(avg))
					return
				}
			}
			Fail(fmt.Sprintf("grade not found: name=%s, class=%s", name, class))
		}

		var eval = func(input object.Object) []object.Delta {
			GinkgoHelper()
			// grade <- grade
			_, err := gradeP.Evaluate(object.Delta{Type: object.Upserted, Object: input})
			Expect(err).NotTo(HaveOccurred())

			// stat <- grade
			ds, err := statP.Evaluate(object.Delta{Type: object.Upserted, Object: input})
			Expect(err).NotTo(HaveOccurred())

			// grade <- stat
			deltas := []object.Delta{}
			for _, d := range ds {
				d.Object.SetKind("stat")
				ds, err := gradeP.Evaluate(d)
				Expect(err).NotTo(HaveOccurred())
				for _, dd := range ds {
					dd.Object.SetKind("grade")
				}
				deltas = append(deltas, ds...)
			}
			return deltas
		}

		It("should init the pipelines", func() {
			var err error
			// create the averages
			jsonData := `
- '@gather':
    - $.class
    - $.grade
- '@project':
    $.metadata.name:
       "@concat": ["stat-", $.class]
    class: $.class
    avg:
      "@div":
         - "@sum": $.grade
         - "@len": $.grade`
			statP, err = newPipeline(jsonData, []string{"grade"})
			Expect(err).NotTo(HaveOccurred())

			// create the grades
			jsonData = `
- '@join': 
    "@eq": [ "$.grade.class", $.stat.class ]
- '@project':
    $.metadata.name: $.grade.metadata.name
    name: $.grade.name
    class: $.grade.class
    grade: $.grade.grade
    avg: $.stat.avg`
			gradeP, err = newPipeline(jsonData, []string{"grade", "stat"})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should backfill the aggregation", func() {
			// grade 1
			deltas := eval(makeGrade("Alice", "Algebra", 95))
			Expect(deltas).To(HaveLen(1))
			checkGrade(deltas, "Alice", "Algebra", 95, float64(95))

			// grade 2
			deltas = eval(makeGrade("Alice", "Physics", 89))
			Expect(deltas).To(HaveLen(1))
			checkGrade(deltas, "Alice", "Physics", 89, float64(89))

			// grade 3
			deltas = eval(makeGrade("Bob", "Physics", 93))
			Expect(deltas).To(HaveLen(2))
			checkGrade(deltas, "Alice", "Physics", 89, float64(91))
			checkGrade(deltas, "Bob", "Physics", 93, float64(91))

			// grade 4
			deltas = eval(makeGrade("Bob", "Algebra", 90))
			Expect(deltas).To(HaveLen(2))
			checkGrade(deltas, "Alice", "Algebra", 95, float64(92.5))
			checkGrade(deltas, "Bob", "Algebra", 90, float64(92.5))
		})

		It("should process updates 1", func() {
			// grade 1
			deltas := eval(makeGrade("Carl", "Algebra", 85))
			Expect(deltas).To(HaveLen(3))
			checkGrade(deltas, "Alice", "Algebra", 95, float64(90))
			checkGrade(deltas, "Bob", "Algebra", 90, float64(90))
			checkGrade(deltas, "Carl", "Algebra", 85, float64(90))

			// grade 2
			deltas = eval(makeGrade("Carl", "Physics", 97))
			Expect(deltas).To(HaveLen(3))
			checkGrade(deltas, "Alice", "Physics", 89, float64(93))
			checkGrade(deltas, "Bob", "Physics", 93, float64(93))
			checkGrade(deltas, "Carl", "Physics", 97, float64(93))
		})

		It("should process updates 2", func() {
			// grade 1
			deltas := eval(makeGrade("David", "Algebra", 96))
			Expect(deltas).To(HaveLen(4))
			checkGrade(deltas, "Alice", "Algebra", 95, float64(91.5))
			checkGrade(deltas, "Bob", "Algebra", 90, float64(91.5))
			checkGrade(deltas, "Carl", "Algebra", 85, float64(91.5))
			checkGrade(deltas, "David", "Algebra", 96, float64(91.5))

			// grade 2
			deltas = eval(makeGrade("David", "Physics", 91))
			Expect(deltas).To(HaveLen(4))
			checkGrade(deltas, "Alice", "Physics", 89, float64(92.5))
			checkGrade(deltas, "Bob", "Physics", 93, float64(92.5))
			checkGrade(deltas, "Carl", "Physics", 97, float64(92.5))
			checkGrade(deltas, "David", "Physics", 91, float64(92.5))
		})
	})
})
