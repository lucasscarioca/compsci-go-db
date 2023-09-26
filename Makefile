include .env
export

assets:
	@tailwindcss -i ./assets/tailwind.css -o ./assets/static/css/styles.css

build: assets
	@go build ./cmd/server/... -o tmp/main

start: build
	@./tmp/main

run: assets
	@go run ./cmd/server/...

test:
	@go test -v ./...

install:
	@npm i -g tailwindcss
	@go mod tidy
