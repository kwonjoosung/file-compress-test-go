package main

import (
	"context"
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
)

// Compression Option - 7z 무압축(Copy) 모드
// 7z은 컨테이너에 미리 설치되어 있어야 하며, /var/task/7za 경로에 위치해야함
const (
	SevenZipCmd        = "/var/task/7za"
	SevenZipFormatFlag = "-t7z"
	SevenZipCopyFlag   = "-m0=Copy"
	SevenZipThreads    = "-mmt=2"
	TempDir            = "/tmp"
	CompressExtension  = ".7z"
)

// static s3 client
var s3Client *s3.Client

// (TODO: 환경 변수로 리전 변겸 & 설정 가능하도록 수정)
func init() {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("ap-northeast-2"),
	)
	if err != nil {
		log.Fatalf("unable to load AWS SDK config: %v", err)
	}
	s3Client = s3.NewFromConfig(cfg)
}

// Request
type FileCompressionForm struct {
	OriginRegion string `json:"originRegion"`
	OriginBucket string `json:"originBucket"`
	OriginKey    string `json:"originKey"`
	TargetRegion string `json:"targetRegion"`
	TargetBucket string `json:"targetBucket"`
	TargetKey    string `json:"targetKey"`
}

// Response
type CompressionResultData struct {
	Result  string `json:"result"`
	Message string `json:"message"`
	Region  string `json:"region"`
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
}

// Entry point for Lambda function
func Handler(ctx context.Context, event FileCompressionForm) (CompressionResultData, error) {
	startTime := time.Now()

	// input validation
	if err := validateRequest(event); err != nil {
		log.Printf("Input validation failed: %v", err)
		return CompressionResultData{}, fmt.Errorf("input validation failed: %w", err)
	}

	// 별도로 Target을 지정하지 않는 경우, Origin 값을 기본 값으로 사용
	targetBucket := event.TargetBucket
	if targetBucket == "" {
		targetBucket = event.OriginBucket
	}

	// TargetKey가 비어있으면 OriginKey의 확장자를 7z 으로 변경하여 사용
	targetKey := event.TargetKey
	if targetKey == "" {
		targetKey = replaceExtension(event.OriginKey, CompressExtension)
	}

	// 임시 저장 위치 지정(원본, 결과)
	inputPath, outputPath := buildTempPaths(event.OriginKey)

	// defer를 사용하여 함수 종료 후 임시 파일 정리
	defer func() {
		cleanupTemp(inputPath, outputPath)
	}()

	// S3에서 원본 파일 다운로드
	start := time.Now()
	originalSize, err := downloadFromS3(ctx, event.OriginBucket, event.OriginKey, inputPath)
	if err != nil {
		log.Printf("Download from S3 failed: %v (took %s)", err, time.Since(start))
		return CompressionResultData{}, fmt.Errorf("download from S3 failed: %w", err)
	}
	log.Printf("Download from S3 completed: %d bytes, took %s", originalSize, time.Since(start))

	// 7z 포맷으로 파일 압축
	start = time.Now()
	if err := compressFile(inputPath, outputPath); err != nil {
		log.Printf("File compression failed: %v (took %s)", err, time.Since(start))
		return CompressionResultData{}, fmt.Errorf("file compression failed: %w", err)
	}
	log.Printf("File compression completed, took %s", time.Since(start))

	// S3에 파일 업로드 (원본과 압축 파일의 Key가 다르면 두개 존재 가능 - 원본은 호출한 측에서 삭제하도록 가이드)
	start = time.Now()
	compressedSize, err := uploadToS3(ctx, targetBucket, targetKey, outputPath)
	if err != nil {
		log.Printf("Upload to S3 failed: %v (took %s)", err, time.Since(start))
		return CompressionResultData{}, fmt.Errorf("upload to S3 failed: %w", err)
	}
	log.Printf("Upload to S3 completed: %d bytes, took %s", compressedSize, time.Since(start))

	totalProcessingTime := time.Since(startTime)
	log.Printf("File processing success. Total processing time: %s", totalProcessingTime)

	// 성공 응답
	return CompressionResultData{
		Result:  "SUCCEED",
		Message: "Compression succeeded",
		Region:  event.TargetRegion,
		Bucket:  targetBucket,
		Key:     targetKey,
	}, nil
}

// validateRequest는 요청 유효성 검증
func validateRequest(event FileCompressionForm) error {
	if event.OriginBucket == "" {
		return fmt.Errorf("origin bucket is required")
	}
	if event.OriginKey == "" {
		return fmt.Errorf("origin key is required")
	}
	// 이미 압축된 파일인지 확인
	if strings.HasSuffix(event.OriginKey, CompressExtension) {
		return fmt.Errorf("cannot compress already compressed file (.7z)")
	}
	return nil
}

// buildTempPaths는 입력 키로부터 /tmp 경로를 생성
func buildTempPaths(originKey string) (string, string) {
	fileName := filepath.Base(originKey)
	base := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	inputPath := filepath.Join(TempDir, fmt.Sprintf("%s", fileName))
	outputPath := filepath.Join(TempDir, fmt.Sprintf("%s%s", base, CompressExtension))

	return inputPath, outputPath
}

// downloadFromS3는 S3 버킷에서 파일을 다운로드하고 파일 크기 반환
func downloadFromS3(ctx context.Context, bucket, key, destPath string) (int64, error) {
	f, err := os.Create(destPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer f.Close()

	// S3에서 파일 다운로드
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
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

// compressFile는 7za 바이너리로 무압축(COPY) 압축 실행
func compressFile(inputPath, outputPath string) error {
	// 7z 바이너리 존재 확인
	if _, err := os.Stat(SevenZipCmd); os.IsNotExist(err) {
		return fmt.Errorf("7z binary not found at %s", SevenZipCmd)
	}

	// 7z 명령어 실행(미리 정의된 옵션 상수 기반으로) (7z 압축은 라이브러리가 아닌 바이너리로 실행)
	cmd := exec.Command(SevenZipCmd, "a", SevenZipFormatFlag, SevenZipCopyFlag,
		SevenZipThreads, outputPath, inputPath)

	// 상세한 출력을 위해 환경변수 설정
	cmd.Env = append(os.Environ(), "LANG=C")

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("7za command failed. Command: %v", cmd.Args)
		log.Printf("7za output: %s", string(out))
		return fmt.Errorf("7za execution failed: %w, output: %s", err, string(out))
	}

	log.Printf("7za compression completed successfully")
	return nil
}

// uploadToS3는 파일을 S3에 업로드하고 업로드된 파일 크기 반환
func uploadToS3(ctx context.Context, bucket, key, sourcePath string) (int64, error) {
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
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})

	if err != nil {
		return 0, fmt.Errorf("failed to put S3 object: %w", err)
	}

	return fileSize, nil
}

// replaceExtension은 파일 확장자를 변경
func replaceExtension(key, newExtension string) string {
	ext := filepath.Ext(key)
	if ext == "" {
		return key + newExtension
	}
	return key[:len(key)-len(ext)] + newExtension
}

// cleanupTemp은 임시 파일을 삭제
func cleanupTemp(paths ...string) {
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			log.Printf("Failed to remove temp file %s: %v", p, err)
		}
	}
}

func main() {
	lambda.Start(Handler)
}
