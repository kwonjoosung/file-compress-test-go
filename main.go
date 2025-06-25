package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Compression Option - 7z 무압축(Copy) 모드
// 7z은 컨테이너에 미리 설치되어 있어야 하며, /var/task/7za 경로에 위치해야함
const (
	SevenZipCmd        = "/var/task/7za" // 7z 바이너리 경로
	SevenZipFormatFlag = "-t7z"          // 압축 포맷
	SevenZipCopyFlag   = "-m0=Copy"      // 무압축 옵션
	TempDir            = "/tmp"
	CompressExtension  = ".7z"
	BufferSize         = 4 * 1024 * 1024
)

// static client map
var (
	s3Clients  = map[string]*s3.Client{}  // 리전별 S3 클라이언트 캐시
	sqsClients = map[string]*sqs.Client{} // 리전별 SQS 클라이언트 캐시
)

// Lambda Request 구조체
type FileCompressionForm struct {
	ProcessUuid    string `json:"processUuid"`
	OriginRegion   string `json:"originRegion"`
	OriginBucket   string `json:"originBucket"`
	OriginKey      string `json:"originKey"`
	TargetRegion   string `json:"targetRegion"`
	TargetBucket   string `json:"targetBucket"`
	TargetKey      string `json:"targetKey"`
	DeleteOriginal bool   `json:"deleteOriginal"`
	QueueRegion    string `json:"queueRegion"`
	QueueUrl       string `json:"queueUrl"`
}

// Result Response 구조체
type CompressionResultData struct {
	Result      string `json:"result"`
	Message     string `json:"message"`
	ProcessUuid string `json:"processUuid"`
	Region      string `json:"region"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
}

// 초기화: 환경 변수로부터 리전 받아서 S3/SQS 클라이언트 생성
func init() {
	s3Region := os.Getenv("DEFAULT_S3_REGION")
	if s3Region == "" {
		s3Region = getLambdaRegion()
		log.Printf("[WARN] DEFAULT_S3_REGION not set, fallback to Lambda region: %s", s3Region)
	}
	s3Clients[s3Region] = createS3Client(s3Region)

	sqsRegion := os.Getenv("DEFAULT_SQS_REGION")
	if sqsRegion == "" {
		sqsRegion = getLambdaRegion()
		log.Printf("[WARN] DEFAULT_SQS_REGION not set, fallback to Lambda region: %s", sqsRegion)
	}
	sqsClients[sqsRegion] = createSQSClient(sqsRegion)
}

// Lambda 엔트리 포인트 핸들러
func Handler(ctx context.Context, event FileCompressionForm) (CompressionResultData, error) {
	startTime := time.Now()

	// request input 유효성 검사
	if err := validateRequest(event); err != nil {
		log.Printf("[ERROR] Invalid request: %v", err)
		return buildErrorResult(event, err), err
	}

	// 기본값 설정 - 별도로 Target을 지정하지 않는 경우, Origin 값을 기본 값으로 사용, TargetKey가 비어있으면 OriginKey의 확장자를 7z 으로 변경하여 사용
	originRegion := defaultIfEmpty(event.OriginRegion, getLambdaRegion())
	targetRegion := defaultIfEmpty(event.TargetRegion, originRegion)
	targetBucket := defaultIfEmpty(event.TargetBucket, event.OriginBucket)
	targetKey := defaultIfEmpty(event.TargetKey, replaceExtension(event.OriginKey, CompressExtension))

	// 임시 파일 경로 설정
	inputPath, outputPath := buildTempPaths(event.OriginKey)
	defer cleanupTemp(inputPath, outputPath)

	// 압축할 파일 다운로드
	s3Client := getS3Client(originRegion)
	start := time.Now()
	originalSize, err := downloadFromS3(ctx, s3Client, event.OriginBucket, event.OriginKey, inputPath)
	if err != nil {
		log.Printf("[ERROR] Download failed: %v (duration: %s)", err, time.Since(start))
		return buildErrorResult(event, err), err
	}
	log.Printf("Download success: %d bytes (duration: %s)", originalSize, time.Since(start))

	// 파일 압축 수행
	start = time.Now()
	if err := compressFile(inputPath, outputPath); err != nil {
		log.Printf("[ERROR] Compression failed: %v (duration: %s)", err, time.Since(start))
		return buildErrorResult(event, err), err
	}
	log.Printf("Compression success (duration: %s)", time.Since(start))

	// 압축된 파일 지정된 버킷에 업로드
	s3Client = getS3Client(targetRegion)
	start = time.Now()
	compressedSize, err := uploadToS3(ctx, s3Client, targetBucket, targetKey, outputPath)
	if err != nil {
		log.Printf("[ERROR] Upload failed: %v (duration: %s)", err, time.Since(start))
		return buildErrorResult(event, err), err
	}
	log.Printf("Upload success: %d bytes (duration: %s)", compressedSize, time.Since(start))

	// 원본 삭제(선택 옵션)
	if event.DeleteOriginal {
		if err := deleteFromS3(ctx, s3Client, event.OriginBucket, event.OriginKey); err != nil {
			log.Printf("[WARN] Failed to delete original file: %v", err)
		} else {
			log.Printf("Original file deleted: %s/%s", event.OriginBucket, event.OriginKey)
		}
	}

	result := CompressionResultData{
		Result:      "SUCCEED",
		Message:     "Compression succeeded",
		Region:      targetRegion,
		Bucket:      targetBucket,
		Key:         targetKey,
		ProcessUuid: event.ProcessUuid,
	}

	// SQS로 결과 전송
	if err := sendResultToQueue(event.QueueRegion, event.QueueUrl, result); err != nil {
		log.Printf("[ERROR] Failed to send SQS message: %v", err)
		return buildErrorResult(event, err), err
	}

	log.Printf("File processing success (total time: %s)", time.Since(startTime))
	return result, nil
}

func defaultIfEmpty(value, def string) string {
	if value == "" {
		return def
	}
	return value
}

func validateRequest(event FileCompressionForm) error {
	if event.OriginBucket == "" || event.OriginKey == "" {
		return fmt.Errorf("origin bucket and key required")
	}
	// 이미 압축된 파일인지 확인
	if strings.HasSuffix(event.OriginKey, CompressExtension) {
		return fmt.Errorf("file is already compressed")
	}
	return nil
}

// S3 버킷에서 파일을 다운로드하고 파일 크기 반환
func downloadFromS3(ctx context.Context, client *s3.Client, bucket, key, destPath string) (int64, error) {
	f, err := os.Create(destPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer f.Close()

	// 파일 다운로드
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get S3 object: %w", err)
	}
	defer resp.Body.Close()

	// 파일에 S3 데이터 복사 (로컬에 임시 저장)
	bytesWritten, err := io.Copy(f, resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to copy S3 data: %w", err)
	}

	return bytesWritten, nil
}

// 7za 바이너리 프로그램으로 압축 수행
func compressFile(inputPath, outputPath string) error {
	if _, err := os.Stat(SevenZipCmd); os.IsNotExist(err) {
		return fmt.Errorf("7za binary not found")
	}
	// 7z 명령어 실행(미리 정의된 옵션 상수 기반으로) (7z 압축은 라이브러리가 아닌 바이너리로 실행)
	cmd := exec.Command(SevenZipCmd, "a", SevenZipFormatFlag, SevenZipCopyFlag, outputPath, inputPath)
	cmd.Env = append(os.Environ(), "LANG=C") // 상세한 출력을 위해 환경변수 설정
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[ERROR] 7za failed: %v\n%s", err, out)
		return fmt.Errorf("7za error: %w", err)
	}
	log.Printf("7za compression successful")
	return nil
}

// 파일을 S3에 업로드하고 업로드된 파일 크기 반환
func uploadToS3(ctx context.Context, client *s3.Client, bucket, key, sourcePath string) (int64, error) {
	f, err := os.Open(sourcePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open source file: %w", err)
	}
	defer f.Close()

	// 파일 크기 확인
	fileInfo, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// S3에 파일 업로드
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to put S3 object: %w", err)
	}

	return fileSize, nil
}

func deleteFromS3(ctx context.Context, client *s3.Client, bucket, key string) error {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

func sendResultToQueue(region, queueUrl string, result CompressionResultData) error {
	client := getSQSClient(region)
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueUrl),
		MessageBody: aws.String(string(body)),
	})
	return err
}

// 입력 키로부터 /tmp 경로를 생성
func buildTempPaths(originKey string) (string, string) {
	fileName := filepath.Base(originKey)
	base := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	inputPath := filepath.Join(TempDir, fileName)
	outputPath := filepath.Join(TempDir, base+CompressExtension)

	return inputPath, outputPath
}

// 파일 확장자 변경 메서드
func replaceExtension(key, newExtension string) string {
	ext := filepath.Ext(key)
	if ext == "" {
		return key + newExtension
	}
	return key[:len(key)-len(ext)] + newExtension
}

// 임시 파일 삭제
func cleanupTemp(paths ...string) {
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			log.Printf("[WARN] Failed to delete temp file %s: %v", p, err)
		}
	}
}

func buildErrorResult(event FileCompressionForm, err error) CompressionResultData {
	return CompressionResultData{
		Result:      "FAILED",
		Message:     err.Error(),
		Region:      event.OriginRegion,
		Bucket:      event.OriginBucket,
		Key:         event.OriginKey,
		ProcessUuid: event.ProcessUuid,
	}
}

func getLambdaRegion() string {
	return os.Getenv("AWS_REGION")
}

func createS3Client(region string) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("[ERROR] Failed to load S3 config for region %s: %v", region, err)
	}
	return s3.NewFromConfig(cfg)
}

func createSQSClient(region string) *sqs.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("[ERROR] Failed to load SQS config for region %s: %v", region, err)
	}
	return sqs.NewFromConfig(cfg)
}

func getS3Client(region string) *s3.Client {
	if client, ok := s3Clients[region]; ok {
		return client
	}
	client := createS3Client(region)
	s3Clients[region] = client
	return client
}

func getSQSClient(region string) *sqs.Client {
	if client, ok := sqsClients[region]; ok {
		return client
	}
	client := createSQSClient(region)
	sqsClients[region] = client
	return client
}

func main() {
	lambda.Start(Handler)
}
