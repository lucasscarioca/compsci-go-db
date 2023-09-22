package db

import (
	"errors"
	"fmt"
	"log"
	"math"
)

type searchKey = string

// Valor da chave de busca e os dados da linha.
type tuple struct {
	Key  searchKey
	Data string
}

// Todas as tuplas.
type table struct {
	Pages []page
}

func (t *table) pushTuple(tuple tuple, pageSize int) (int, error) {
	for pageIndex, page := range t.Pages {
		if len(page.Tuples) >= pageSize {
			continue
		}
		page.Tuples = append(page.Tuples, tuple)
		return pageIndex, nil
	}
	return 0, errors.New("failed to push tuple in table. Tuple:" + tuple.Key)
}

// Alocação física da tabela.
type page struct {
	Tuples []tuple
}

// Mapeia chaves de busca em endereços de páginas.
// A chave de busca e o endereço da página onde a tupla foi armazenada são adicionadas ao bucket
type bucket struct {
	Map            []bucketMap // key -> page array index
	OverflowBucket *bucket
}

type bucketMap struct {
	Value    searchKey
	PageAddr int
}

func (b *bucket) handleOverflow(h *HashIndex, payload bucketMap) {
	if len(b.Map) >= BUCKET_SIZE {
		h.OverflowCount++
		if b.OverflowBucket == nil {
			b.OverflowBucket = &bucket{}
		}
		b.OverflowBucket.handleOverflow(h, payload)
	}
	b.Map = append(b.Map, payload)
}

func (b *bucket) FindPageAddr(key searchKey) (int, error) {
	for _, bMap := range b.Map {
		if bMap.Value == key {
			return bMap.PageAddr, nil
		}
	}
	if b.OverflowBucket != nil && len(b.OverflowBucket.Map) > 0 {
		return b.OverflowBucket.FindPageAddr(key)
	}
	return 0, errors.New("key not found")
}

type HashIndex struct {
	NR             int      // Número de tuplas
	FR             int      // Número de tuplas por bucket
	NB             int      // NR/FR
	Buckets        []bucket // length = NB
	OverflowCount  int
	CollisionCount int
}

func (h *HashIndex) pushKey(bucketAddr int, key searchKey, pageAddr int) {
	// handle overflow
	if len(h.Buckets[bucketAddr].Map) >= BUCKET_SIZE {
		h.CollisionCount++
		h.Buckets[bucketAddr].handleOverflow(h, bucketMap{key, pageAddr})
		return
	}
	h.Buckets[bucketAddr].Map = append(h.Buckets[bucketAddr].Map, bucketMap{key, pageAddr})
}

// Recebe a palavra e indica o bucket a ser guardado
func Hash(key searchKey, nb int) int {
	var res int
	// (k^0 * (first char)) + (k^1 * (second char)) + (k^2 * (third char)) + ...
	for i := 0; i < len(key); i++ {
		res += int(key[i]) * int(math.Pow(31, float64(i)))
	}

	return int(math.Abs((math.Mod(float64(res), float64(nb)))))
}

const BUCKET_SIZE = 200

var Hashtable table

func NewHashIndex(pageSize int, dataArr []string) HashIndex {
	nb := int(math.Ceil(float64(len(dataArr)) / float64(BUCKET_SIZE)))
	// Create Hash Index Struct
	hashIndex := HashIndex{
		NR:      len(dataArr),
		FR:      BUCKET_SIZE,
		NB:      nb,
		Buckets: make([]bucket, nb),
	}

	// Create Table and Pages
	pagesQtty := int(math.Ceil(float64(len(dataArr)) / float64(pageSize)))
	Hashtable = table{
		Pages: make([]page, pagesQtty),
	}

	for _, value := range dataArr {
		// Create tuple
		newTuple := tuple{value, value}
		// Generate Hash
		bucketAddr := Hash(newTuple.Key, nb)
		// Find available page and push tuple
		pageAddr, err := Hashtable.pushTuple(newTuple, pageSize)
		if err != nil {
			log.Fatal(err)
		}
		// Update Bucket with searchKey and pageAddress
		hashIndex.pushKey(bucketAddr, newTuple.Key, pageAddr)
		// Hashtable.Pages[pageAddr].Tuples[valueIndex] = newTuple
	}

	// Create Tuples
	// for pageIndex, page := range Hashtable.Pages {
	// 	page.Tuples = make([]tuple, tuplesPerPage)
	// 	Hashtable.Pages[pageIndex] = page
	// 	// Add tuple data
	// 	// fmt.Println(len(Hashtable.Pages), tuplesPerPage)
	// 	for tupleIndex, tuple := range page.Tuples {
	// 		tuple.Key = dataArr[tupleIndex]
	// 		tuple.Data = tuple.Key

	// 		// Generate Hash
	// 		bucketAddr := Hash(tuple.Key, nb)
	// 		if tuple.Key == "scrambler" {
	// 			fmt.Println(bucketAddr)
	// 		}
	// 		// Update Bucket with searchKey and pageAddress
	// 		hashIndex.pushKey(bucketAddr, tuple.Key, pageIndex)
	// 		Hashtable.Pages[pageIndex].Tuples[tupleIndex] = tuple
	// 	}
	// }
	return hashIndex
}

func (h *HashIndex) Find(key searchKey) (tuple, error) {
	bucketAddr := Hash(key, h.NB)
	pageAddr, err := h.Buckets[bucketAddr].FindPageAddr(key)
	if err != nil {
		fmt.Println(err.Error())
		return tuple{}, err
	}
	for tupleIndex, tuple := range Hashtable.Pages[pageAddr].Tuples {
		if tuple.Key == key {
			fmt.Println("Value found on Page: ", pageAddr)
			fmt.Println("On Page position: ", tupleIndex)
			return tuple, nil
		}
	}
	return tuple{}, fmt.Errorf("page address found: %v. But value not found in page", pageAddr)
}
