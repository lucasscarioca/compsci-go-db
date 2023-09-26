package core

import "errors"

// Todas as tuplas.
type table struct {
	Pages []page
}

func (t *table) pushTuple(tup tuple, pageSize int) (int, error) {
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
