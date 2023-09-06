package db

import (
	"errors"
	"fmt"
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
		b.OverflowBucket.handleOverflow(h, payload)
	}
	b.Map = append(b.Map, payload)
}

func (b *bucket) Find(key searchKey) (int, error) {
	for _, bMap := range b.Map {
		if bMap.Value == key {
			return bMap.PageAddr, nil
		}
	}
	if len(b.OverflowBucket.Map) > 0 {
		return b.OverflowBucket.Find(key)
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

// Mapeia uma chave de busca em um endereço de bucket
// Calcula o endereço do bucket (indice do bucket) a partir da chave de busca
// Recebe a palavra e indica o bucket a ser guardado
func Hash(key searchKey, nb int) int {
	var res int
	// Sum ASCII chars
	for i := 0; i < len(key); i++ {
		res += int(key[i]) * int(math.Pow(31, float64(i)))
	}

	return int(math.Mod(float64(res), float64(nb)))
}

const BUCKET_SIZE = 5

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

	// Create Tuples
	for pageIndex, page := range Hashtable.Pages {
		page.Tuples = make([]tuple, pageSize)
		// Add tuple data
		for tupleIndex, tuple := range page.Tuples {
			fmt.Println(pageIndex+tupleIndex, dataArr[pageIndex+tupleIndex])
			tuple.Key = dataArr[pageIndex+tupleIndex]
			tuple.Data = tuple.Key

			// Generate Hash
			fmt.Println(Hash(tuple.Key, nb))
			bucketAddr := int(math.Abs(float64(Hash(tuple.Key, nb))))
			// Update Bucket with searchKey and pageAddress
			hashIndex.pushKey(bucketAddr, tuple.Key, pageIndex)
		}
	}
	return HashIndex{}
}

func (h *HashIndex) Find(key searchKey) (tuple, error) {
	bucketAddr := Hash(key, h.NB)
	pageAddr, err := h.Buckets[bucketAddr].Find(key)
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
