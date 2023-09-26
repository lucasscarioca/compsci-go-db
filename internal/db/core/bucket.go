package core

import "errors"

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
	if b.OverflowBucket == nil {
		b.OverflowBucket = &bucket{}
		h.OverflowCount++
	}
	if len(b.OverflowBucket.Map) >= BUCKET_SIZE {
		b.OverflowBucket.handleOverflow(h, payload)
	}
	b.OverflowBucket.Map = append(b.OverflowBucket.Map, payload)
}

func (b *bucket) findPageAddr(key searchKey) (int, error) {
	for _, bMap := range b.Map {
		if bMap.Value == key {
			return bMap.PageAddr, nil
		}
	}
	if b.OverflowBucket != nil && len(b.OverflowBucket.Map) > 0 {
		return b.OverflowBucket.findPageAddr(key)
	}
	return 0, errors.New("key not found")
}
