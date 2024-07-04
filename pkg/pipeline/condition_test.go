package pipeline

// import (
// 	"encoding/json"

// 	. "github.com/onsi/ginkgo/v2"
// 	. "github.com/onsi/gomega"
// )

// var _ = Describe("Conditionals", func() {
// 	Describe("Deserializing conditionals", func() {
// 		It("should allow simple eq conditionals to be deserialized from JSON", func() {
// 			jsonData := `{"@eq": ["$x", 10]}`

// 			var condition Condition
// 			err := json.Unmarshal([]byte(jsonData), &condition)
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(condition).To(Equal(EqCondition{Eq: []Expression{"$x", "10"}}))
// 		})
// 		It("should allow simple lte conditionals to be deserialized from JSON", func() {
// 			jsonData := `{"@eq": ["$x", 10]}`

// 			var condition Condition
// 			err := json.Unmarshal([]byte(jsonData), &condition)
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(condition).To(Equal(LteCondition{Lte: []Expression{"$x", "10"}}))
// 		})
// 		It("should allow complex conditionals to be deserialized from JSON", func() {
// 			jsonData := `
// 	{
// 		"@and": [
// 			{"@eq": ["$x", 10]},
// 			{
// 				"@or": [
// 					{"@gt": ["$y", 20]},
// 					{"@lt": ["$y", 10]}
// 				]
// 			}
// 		]
// 	}`

// 			var condition Condition
// 			err := json.Unmarshal([]byte(jsonData), &condition)
// 			Expect(err).NotTo(HaveOccurred())
// 			Expect(condition).To(Equal(AndCondition{
// 				And: []Condition{
// 					EqCondition{Eq: []Expression{"$x", "10"}},
// 					OrCondition{Or: []Condition{
// 						GtCondition{Gt: []Expression{"$y", "20"}},
// 						LtCondition{Lt: []Expression{"$y", "10"}},
// 					}},
// 				},
// 			}))
// 		})
// 	})
// })
