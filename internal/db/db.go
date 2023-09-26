package db

import (
	"bufio"
	"fmt"
	"os"

	"github.com/lucasscarioca/custom-db/internal/db/core"
	"github.com/lucasscarioca/custom-db/internal/db/models"
)

const pageSize = 200

type database struct {
	hashIndex core.HashIndex
	hashTable core.Table
}

var DB database

func Init() error {
	file, err := os.Open("./data/words.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var lines []string

	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	DB.hashIndex = core.NewHashIndex(pageSize, lines)
	DB.hashTable = core.Hashtable

	return nil
}

func (d *database) Find(input string) (models.HashIndexFindResponse, error) {
	return d.hashIndex.Find(input)
}

func (d *database) GetHashIndexStats() models.HashIndexStatsResponse {
	return models.HashIndexStatsResponse{
		NR:             d.hashIndex.NR,
		FR:             d.hashIndex.FR,
		NB:             d.hashIndex.NB,
		PagesQtty:      d.hashIndex.PagesQtty,
		OverflowCount:  d.hashIndex.OverflowCount,
		OverflowPct:    fmt.Sprintf("%.2f", (float64(d.hashIndex.OverflowCount)/float64(core.BUCKET_SIZE))*100),
		CollisionCount: d.hashIndex.CollisionCount,
		CollisionPct:   fmt.Sprintf("%.2f", (float64(d.hashIndex.CollisionCount)/float64(d.hashIndex.NR))*100),
	}
}

func (d *database) TableScan(n int) models.TableScanResponse {
	return d.hashTable.Scan(n)
}
