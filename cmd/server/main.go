package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/labstack/echo/v4"
	"github.com/lucasscarioca/custom-db/internal/db/core"
	"github.com/lucasscarioca/custom-db/internal/http/middlewares"
)

func main() {
	file, err := os.Open("./data/words.txt")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	e := echo.New()
	middlewares.Mount(e)

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

	hashIndex := core.NewHashIndex(200, lines)

	var input string
	fmt.Print("Enter key: ")
	fmt.Scanln(&input)
	tuple, err := hashIndex.Find(input)
	if err != nil {
		fmt.Println("find key error:", err)
	}
	fmt.Println(tuple)
}
