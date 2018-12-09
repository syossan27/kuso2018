#! /usr/bin/make

build:
	@GOOS=linux go build -o kuso kuso.go

zip: build
	@build-lambda-zip -o kuso.zip kuso
