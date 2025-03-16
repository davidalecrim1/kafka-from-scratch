test-parse-correlation-id:
	echo -n "00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C

test-parse-the-api-version:
	echo -n "000000230012674a4f74d28b00096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C
	
run:
	@cd broker && \
	go run cmd/main.go

tests:
	@cd broker && \
	go test ./... -coverprofile=coverage.out  -coverpkg=./internal/...,./server/... -v -race

coverage:
	@cd broker && \
	go tool cover -html=coverage.out
