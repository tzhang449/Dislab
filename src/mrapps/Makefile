all: wc.so crash.so

wc.so: wc.go
	go build -buildmode=plugin wc.go

crash.so: crash.go
	go build -buildmode=plugin crash.go

.PHONY:clean

clean:
	rm wc.so