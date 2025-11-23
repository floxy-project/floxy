NAMESPACE=floxy

RED="\033[0;31m"
GREEN="\033[1;32m"
NOCOLOR="\033[0m"

.DEFAULT_GOAL := help

#
# Extra targets
#
-include dev/dev.mk

#
# Local targets
#

.PHONY: help
help: ## Prints this message
	@echo "$$(grep -hE '^\S+:.*##' $(MAKEFILE_LIST) | sed -e 's/:.*##\s*/:/' -e 's/^\(.\+\):\(.*\)/\\x1b[36m\1\\x1b[m:\2/' | column -c2 -t -s :)"

.PHONY: test
test: ## Run all tests
	@go test -tags=chaos -cover -coverprofile=coverage.out -v ./...
	@go tool cover -html=coverage.out -o coverage.html

.PHONY: test.sqlite
test.sqlite: ## Run all tests with SQLite store
	@go test -tags=chaos -tags sqlite -cover -coverprofile=coverage.out -v ./...
	@go tool cover -html=coverage.out -o coverage.html

.PHONY: test.short
test.short: ## Run short unit tests
	@go test -tags=chaos -short -cover -coverprofile=coverage.out -v ./...
	@go tool cover -html=coverage.out -o coverage.html
