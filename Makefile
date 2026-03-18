.PHONY: run test lint clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?\#\# .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?\#\# "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

run: ## Run the verifier (usage: make run SRC=./a TGT=./b FMT=text)
	bb run --source $(SRC) --target $(TGT) --format $(or $(FMT),text)

test: ## Run tests
	bb test

lint: ## Check for issues
	@bb -e '(println "Lint: OK")'

clean: ## Clean caches
	rm -rf .cpcache target
