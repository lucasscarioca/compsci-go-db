package core

import (
	"errors"

	"github.com/lucasscarioca/custom-db/internal/db/models"
)

// Todas as tuplas.
type Table struct {
	Pages []page
}

func (t *Table) pushTuple(tup tuple, pageSize int) (int, error) {
	for pageIndex := range t.Pages {
		if t.Pages[pageIndex].Tuples == nil {
			t.Pages[pageIndex].Tuples = []tuple{}
		}
		if len(t.Pages[pageIndex].Tuples) >= pageSize {
			continue
		}
		t.Pages[pageIndex].Tuples = append(t.Pages[pageIndex].Tuples, tup)
		// fmt.Println("pushed to page", t.Pages[pageIndex].Tuples)
		return pageIndex, nil
	}
	return 0, errors.New("failed to push tuple in table. Tuple:" + tup.Key)
}

func (t *Table) Scan(n int) models.TableScanResponse {
	var i int
	var data []string
	for _, page := range t.Pages {
		for _, tup := range page.Tuples {
			if i > n {
				return models.TableScanResponse{Data: data}
			}
			data = append(data, tup.Data)
			i++
		}
	}
	return models.TableScanResponse{Data: data}
}
