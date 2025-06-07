package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// 7z 무압축(COPY) 옵션 상수
const (
	SevenZipCmd        = "7za"
	SevenZipFormatFlag = "-t7z"
	SevenZipCopyFlag   = "-m0=Copy"
	TempDir            = "/tmp"
)

// 전역 S3 클라이언트
var s3Client *s3.Client

func init() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("unable to load AWS SDK config: %v", err)
	}
	s3Client = s3.NewFromConfig(cfg)
}

// Lambda 입력 폼
type FileCompressionForm struct {
	OriginBucket string `json:"originBucket"`
	OriginKey    string `json:"originKey"`
	TargetBucket string `json:"targetBucket"`
	TargetKey    string `json:"targetKey"`
}

// Lambda 출력 폼
type CompressionResultData struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
}

// Handler는 Lambda 엔트리포인트
func Handler(ctx context.Context, event FileCompressionForm) (CompressionResultData, error) {
	inputPath, outputPath := buildTempPaths(event.OriginKey)

	// 1) 다운로드
	start := time.Now()
	if err := downloadFromS3(ctx, event.OriginBucket, event.OriginKey, inputPath); err != nil {
		log.Printf("downloadFromS3 error: %v (took %s)", err, time.Since(start))
		return CompressionResultData{}, err
	}
	log.Printf("downloadFromS3 took %s", time.Since(start))

	// 2) 압축
	start = time.Now()
	if err := compressFile(inputPath, outputPath); err != nil {
		log.Printf("compressFile error: %v (took %s)", err, time.Since(start))
		return CompressionResultData{}, err
	}
	log.Printf("compressFile took %s", time.Since(start))

	// 3) 업로드
	start = time.Now()
	if err := uploadToS3(ctx, event.TargetBucket, event.TargetKey, outputPath); err != nil {
		log.Printf("uploadToS3 error: %v (took %s)", err, time.Since(start))
		return CompressionResultData{}, err
	}
	log.Printf("uploadToS3 took %s", time.Since(start))

	// 4) 임시 파일 정리
	cleanupTemp(inputPath, outputPath)

	// 성공 응답
	return CompressionResultData{
		Status:  "SUCCEED",
		Message: "Compression succeeded",
		Bucket:  event.TargetBucket,
		Key:     event.TargetKey,
	}, nil
}

// buildTempPaths는 입력 키로부터 /tmp 경로를 생성
func buildTempPaths(originKey string) (string, string) {
	fileName := filepath.Base(originKey)
	base := fileName[:len(fileName)-len(filepath.Ext(fileName))]
	return filepath.Join(TempDir, fileName), filepath.Join(TempDir, base+".7z")
}

// downloadFromS3는 S3 버킷에서 파일을 다운로드
func downloadFromS3(ctx context.Context, bucket, key, destPath string) error {
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(f, resp.Body)
	return err
}

// compressFile는 7za 바이너리로 무압축(COPY) 압축 실행
func compressFile(inputPath, outputPath string) error {
	cmd := exec.Command(SevenZipCmd, "a", SevenZipFormatFlag, SevenZipCopyFlag, outputPath, inputPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Println("7za output:", string(out))
	}
	return err
}

// uploadToS3는 파일을 S3에 업로드
func uploadToS3(ctx context.Context, bucket, key, sourcePath string) error {
	f, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: f})
	return err
}

// cleanupTemp은 임시 파일을 삭제
func cleanupTemp(paths ...string) {
	for _, p := range paths {
		os.Remove(p)
	}
}

func main() {
	lambda.Start(Handler)
}
