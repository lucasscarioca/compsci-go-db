package core

import (
	"fmt"
	"log"
	"math"
)

const BUCKET_SIZE = 800

var Hashtable table

// Recebe a palavra e indica o bucket a ser guardado
func Hash(key searchKey, nb int) int {
	var res int
	// (k^0 * (first char)) + (k^1 * (second char)) + (k^2 * (third char)) + ...
	for i := 0; i < len(key); i++ {
		res += int(key[i]) * int(math.Pow(7, float64(i)))
	}

	return int(math.Abs((math.Mod(float64(res), float64(nb)))))
}

type HashIndex struct {
	NR             int      // Número de tuplas
	FR             int      // Número de tuplas por bucket
	NB             int      // NR/FR
	Buckets        []bucket // length = NB
	OverflowCount  int
	CollisionCount int
}

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
	// fmt.Println("pagesQtty", pagesQtty)
	// fmt.Println("nb", nb)
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
		// fmt.Printf("\ntuple: %s\nbucketAddr: %v\npageAddr: %v\n", value, bucketAddr, pageAddr)
	}

	fmt.Println("Hashtable populated and HashIndex created!")
	fmt.Printf(
		"\nNR: %v\nFR: %v\nNB: %v\nNumber of pages: %v\n\nOverflow count: %v\nOverflow percentage: %.2f%%\nCollision count: %v\nCollision percentage: %.2f%%\n\n",
		hashIndex.NR,
		hashIndex.FR,
		hashIndex.NB,
		pagesQtty,
		hashIndex.OverflowCount,
		(float64(hashIndex.OverflowCount)/float64(BUCKET_SIZE))*100,
		hashIndex.CollisionCount,
		(float64(hashIndex.CollisionCount)/float64(len(dataArr)))*100,
	)
	return hashIndex
}

func (h *HashIndex) pushKey(bucketAddr int, key searchKey, pageAddr int) {
	// handle overflow
	if len(h.Buckets[bucketAddr].Map) >= BUCKET_SIZE {
		// fmt.Println("Collision on bucket:", bucketAddr)
		// fmt.Println("Current bucket size:", len(h.Buckets[bucketAddr].Map))
		// fmt.Println(h.Buckets[bucketAddr].Map)
		h.CollisionCount++
		h.Buckets[bucketAddr].handleOverflow(h, bucketMap{key, pageAddr})
		return
	}
	h.Buckets[bucketAddr].Map = append(h.Buckets[bucketAddr].Map, bucketMap{key, pageAddr})
}

func (h *HashIndex) Find(key searchKey) (tuple, error) {
	bucketAddr := Hash(key, h.NB)
	pageAddr, err := h.Buckets[bucketAddr].findPageAddr(key)
	// fmt.Printf("\nBucketAddr: %v\nPageAddr: %v\n", bucketAddr, pageAddr)
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
