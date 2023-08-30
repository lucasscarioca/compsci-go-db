package infra

import (
	"errors"
	"fmt"
	"math"
)

type searchKey = string

// Valor da chave de busca e os dados da linha.
type Tuple struct {
	Key  searchKey
	Data string
}

// Todas as tuplas.
type Table struct {
	Pages []Page
}

// Alocação física da tabela.
type Page struct {
	Tuples []Tuple
}

// Mapeia chaves de busca em endereços de páginas.
// A chave de busca e o endereço da página onde a tupla foi armazenada são adicionadas ao bucket
type Bucket struct {
	Map            []BucketMap // key -> page array index
	OverflowBucket *Bucket
}

type BucketMap struct {
	Value    searchKey
	PageAddr int
}

func (b *Bucket) handleOverflow(h *HashIndex, payload BucketMap) {
	if len(b.Map) >= BUCKET_SIZE {
		h.OverflowCount++
		b.OverflowBucket.handleOverflow(h, payload)
	}
	b.Map = append(b.Map, payload)
}

func (b *Bucket) Find(key searchKey) (int, error) {
	for _, bMap := range b.Map {
		if bMap.Value == key {
			return bMap.PageAddr, nil
		}
	}
	if len(b.OverflowBucket.Map) > 0 {
		return b.OverflowBucket.Find(key)
	}
	return 0, errors.New("Key not found!")
}

type HashIndex struct {
	NR             int      // Número de tuplas
	FR             int      // Número de tuplas por bucket
	Buckets        []Bucket // length = NR/FR
	OverflowCount  int
	CollisionCount int
}

func (h *HashIndex) pushKey(bucketAddr int, key searchKey, pageAddr int) {
	// handle overflow
	if len(h.Buckets[bucketAddr].Map) >= BUCKET_SIZE {
		h.CollisionCount++
		h.Buckets[bucketAddr].handleOverflow(h, BucketMap{key, pageAddr})
		return
	}
	h.Buckets[bucketAddr].Map = append(h.Buckets[bucketAddr].Map, BucketMap{key, pageAddr})
}

// Mapeia uma chave de busca em um endereço de bucket
// Calcula o endereço do bucket (indice do bucket) a partir da chave de busca
// Recebe a palavra e indica o bucket a ser guardado
func Hash(key searchKey) int {
	return 0
}

const BUCKET_SIZE = 50

var table Table

func NewHashIndex(pageSize int, dataArr []string) HashIndex {
	// Create Hash Index Struct
	hashIndex := HashIndex{
		NR:      len(dataArr),
		FR:      BUCKET_SIZE,
		Buckets: make([]Bucket, int(math.Ceil(float64(len(dataArr))/float64(BUCKET_SIZE)))),
	}

	// Create Table and Pages
	pagesQtty := int(math.Ceil(float64(len(dataArr)) / float64(pageSize)))
	table = Table{
		Pages: make([]Page, pagesQtty),
	}

	// Create Tuples
	for pageIndex, page := range table.Pages {
		page.Tuples = make([]Tuple, pageSize)
		// Add tuple data
		for tupleIndex, tuple := range page.Tuples {
			tuple.Key = dataArr[pageIndex+tupleIndex]
			tuple.Data = tuple.Key

			// Generate Hash
			bucketAddr := Hash(tuple.Key)
			// Update Bucket with searchKey and pageAddress
			hashIndex.pushKey(bucketAddr, tuple.Key, pageIndex)
		}
	}
	// Para cada tupla ->
	// 		Aplicar função Hash para gerar a searchKey
	// 		Salvar tupla na Tabela
	//		Obter endereço da página a partir do local de armazenamento da tupla na Tabela
	// 		Salvar searchKey e endereço da página no Bucket apontado pela função Hash
	// Informar a pagina encontrada e custo operacional de encontrar a chave na pagina
	return HashIndex{}
}

func (h *HashIndex) Find(key searchKey) (Tuple, error) {
	bucketAddr := Hash(key)
	pageAddr, err := h.Buckets[bucketAddr].Find(key)
	if err != nil {
		fmt.Println(err.Error())
		return Tuple{}, err
	}
	for tupleIndex, tuple := range table.Pages[pageAddr].Tuples {
		if tuple.Key == key {
			fmt.Println("Value found on Page: ", pageAddr)
			fmt.Println("On Page position: ", tupleIndex)
			return tuple, nil
		}
	}
	return Tuple{}, errors.New(fmt.Sprintf("Page address found: %v. But value not found in page!", pageAddr))
}
