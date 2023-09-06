package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/lucasscarioca/custom-db/db"
)

func main() {
	file, err := os.Open("./data/words_test.txt")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var lines []string

	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	db.NewHashIndex(50, lines)
}
