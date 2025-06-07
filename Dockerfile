FROM --platform=linux/amd64 golang:1.24 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main main.go

# 2단계: 최종 Lambda 이미지
FROM --platform=linux/amd64 public.ecr.aws/lambda/go:1

# 7z 정적 바이너리 직접 다운로드
RUN curl -L -o /tmp/7z.tar.xz https://www.7-zip.org/a/7z2301-linux-x64.tar.xz && \
    cd /tmp && \
    tar -xf 7z.tar.xz && \
    cp 7zz /var/task/7za && \
    chmod +x /var/task/7za && \
    rm -rf /tmp/*

# 빌드된 Go 실행 파일 복사
COPY --from=builder /app/main ${LAMBDA_TASK_ROOT}/main

CMD ["main"]