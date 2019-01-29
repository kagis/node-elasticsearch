test:
	docker run --rm -v "$$PWD":/app -w /app -e ELASTICSEARCH=http://192.168.55.102:8502 node:11-alpine node test.js
