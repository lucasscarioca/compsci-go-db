package infra

import "math"

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
	Content        map[searchKey]int // key -> page array index
	OverflowCount  int
	CollisionCount int
	OverflowBucket *Bucket
}

type HashIndex struct {
	NR      int      // Número de tuplas
	FR      int      // Número de tuplas por bucket
	Buckets []Bucket // length = NR/FR
}

// Mapeia uma chave de busca em um endereço de bucket
// Calcula o endereço do bucket (indice do bucket) a partir da chave de busca
// Recebe a palavra e indica o bucket a ser guardado
// COLISÃO: Bucket cheio -> overflow bucket
func Hash(key searchKey) int {
	return 0
}

const BUCKET_SIZE = 50

func NewHashIndex(pageSize int, dataArr []string) HashIndex {
	hashIndex := HashIndex{
		NR:      len(dataArr),
		FR:      BUCKET_SIZE,
		Buckets: make([]Bucket, int(math.Ceil(float64(len(dataArr))/float64(BUCKET_SIZE)))),
	}
	pagesQtty := int(math.Ceil(float64(len(dataArr)) / float64(pageSize)))
	table := Table{
		Pages: make([]Page, pagesQtty),
	}
	for pageIndex, page := range table.Pages {
		page.Tuples = make([]Tuple, pageSize)
		for tupleIndex, tuple := range page.Tuples {
			tuple.Key = dataArr[pageIndex+tupleIndex]
			tuple.Data = tuple.Key
			bucketIndex = Hash(tuple.Key)
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
