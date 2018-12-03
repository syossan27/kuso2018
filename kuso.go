package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"strconv"
	"time"
)

type (
	Param struct {
		Height int
		Age int
		Bust int
		Cup string
		West int
		Hip int
	}

	Response struct {
		Name string `json:"name"`
		Image string `json:"image"`
		Height string `json:"height"`
		Age string `json:"age"`
		Bust string `json:"bust"`
		Cup string `json:"cup"`
		West string `json:"west"`
		Hip string `json:"hip"`
	}
)

func kuso(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion("ap-northeast-1")))
	svc := s3.New(sess)

	// Request Params
	height, err := strconv.Atoi(req.QueryStringParameters["height"])
	age, err := strconv.Atoi(req.QueryStringParameters["age"])
	bust, err := strconv.Atoi(req.QueryStringParameters["bust"])
	cup := req.QueryStringParameters["cup"]
	west, err := strconv.Atoi(req.QueryStringParameters["west"])
	hip, err := strconv.Atoi(req.QueryStringParameters["hip"])

	// New Param struct
	param := Param{
		Height: height,
		Age: age,
		Bust: bust,
		Cup: cup,
		West: west,
		Hip: hip,
	}

	responses, err := fetchDataFromCSV(svc, param)
	if err != nil {
		return errorResponse(), err
	}

	resultJSON, err := json.Marshal(responses)
	if err != nil {
		fmt.Printf("Error: %+v\n", responses)
		return errorResponse(), err
	}

	return successResponse(string(resultJSON))
}

func fetchDataFromCSV(svc *s3.S3, param Param) ([]Response, error){
	// S3からCSV読み込み
	params := &s3.SelectObjectContentInput{
		Bucket:          aws.String("kuso2018"),
		Key:             aws.String("av.csv"),
		ExpressionType:  aws.String(s3.ExpressionTypeSql),
		Expression:      aws.String("SELECT * FROM S3Object"),
		RequestProgress: &s3.RequestProgress{},
		InputSerialization: &s3.InputSerialization{
			CompressionType: aws.String("NONE"),
			CSV: &s3.CSVInput{
				FileHeaderInfo: aws.String(s3.FileHeaderInfoUse),
				FieldDelimiter: aws.String(","),
				AllowQuotedRecordDelimiter: aws.Bool(true),
			},
		},
		OutputSerialization: &s3.OutputSerialization{
			CSV: &s3.CSVOutput{
				FieldDelimiter: aws.String(","),
			},
		},
	}

	resp, err := svc.SelectObjectContent(params)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := resp.EventStream.Close()
		if err != nil {
			panic(err)
		}
	}()

	var responses []Response
	var payloads []byte
	for event := range resp.EventStream.Events() {
		// メッセージタイプ（イベントのタイプ）が ``Records`` の場合にメッセージからデータを取り出す
		switch e := event.(type) {
		case *s3.RecordsEvent:
			payloads = append(payloads, e.Payload...)
		case *s3.EndEvent:
			break
		}
	}
	if err := resp.EventStream.Err(); err != nil {
		return nil, err
	}

	br := bytes.NewReader(payloads)
	r := csv.NewReader(br)
	r.FieldsPerRecord = -1
	for {
		line, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		var age string
		if line[3] == "" {
			age = "-"
		} else {
			birthParse, err := time.Parse("2006-01-02", line[3])
			if err != nil {
				panic(err)
			}
			age, err = calcAge(birthParse)
			if err != nil {
				panic(err)
			}
		}

		response := Response{
			Name: line[0],
			Image: line[1],
			Height: line[2],
			Age: age,
			Bust: line[4],
			Cup: line[5],
			West: line[6],
			Hip: line[7],
		}

		//fmt.Println("Count: ", count)
		//count++
		//if len(line) == 1 {
		//	//for _, v := range line {
		//	//	fmt.Println(v)
		//	//}
		//	fmt.Println(len(e.Payload))
		//}

		responses = append(responses, response)
	}

	return responses, nil
}

func calcAge(t time.Time) (string, error) {
	// 現在日時を数値のみでフォーマット (YYYYMMDD)
	dateFormatOnlyNumber := "20060102" // YYYYMMDD

	now := time.Now().Format(dateFormatOnlyNumber)
	birthday := t.Format(dateFormatOnlyNumber)

	// 日付文字列をそのまま数値化
	nowInt, err := strconv.Atoi(now)
	if err != nil {
		return "-", err
	}
	birthdayInt, err := strconv.Atoi(birthday)
	if err != nil {
		return "-", err
	}

	// (今日の日付 - 誕生日) / 10000 = 年齢
	age := (nowInt - birthdayInt) / 10000
	return strconv.Itoa(age), nil
}

func successResponse(body string) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{StatusCode: 200, Body: body}, nil
}

func errorResponse() events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Internal Server Error"}
}

func main() {
	lambda.Start(kuso)
}
