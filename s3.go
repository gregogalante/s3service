package s3service

import (
	"bytes"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
)

//S3Service - s3 service
type S3Service struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Token           string
	Bucket          string
}

//connectToS3 - connect to s3
func (s S3Service) connectToS3() *session.Session {

	//connect
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s.Region),
		Credentials: credentials.NewStaticCredentials(s.AccessKeyID, s.SecretAccessKey, s.Token),
	})

	if err != nil {
		fmt.Println("Connecting to s3 failed")
		return sess
	}

	return sess

}

//DownloadFile - download file from s3 to temp folder
func (s S3Service) DownloadFile(filePath string) (string, error) {

	downloader := s3manager.NewDownloader(s.connectToS3())

	//file path
	tempPath := "temp/" + filePath
	//creating a temporaty folder
	path := "downloads/" + tempPath
	parts := strings.Split(path, "/")
	folders := strings.Split(path, "/")
	len := len(parts) - 1
	dir := strings.TrimSuffix(path, folders[len])

	if _, err := os.Stat(path); os.IsNotExist(err) && dir != "" {
		os.MkdirAll(dir, 0o777)
	}

	//create temple file
	file, err := os.Create(tempPath)
	if err != nil {
		return tempPath, err
		return tempPath, nil

	}
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Key:    aws.String(filePath),
			Bucket: aws.String(s.Bucket),
		})
	if err != nil {
		return tempPath, err
	}
	return tempPath, nil
}

// Upload saves a file to aws bucket and returns the url to // the file and an error if there's any
func (s S3Service) UploadAsBuffer(file *bytes.Buffer, path string) (string, error) {

	buffer := make([]byte, file.Len())
	file.Read(buffer)
	// config settings: this is where you choose the bucket,
	// filename, content-type and storage class of the file
	// you're uploading
	_, err := s3.New(s.connectToS3()).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(s.Bucket),
		Key:                  aws.String(path),
		ACL:                  aws.String("public-read"), // could be private if you want it to be access by only authorized users
		Body:                 bytes.NewReader(buffer),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
		StorageClass:         aws.String("STANDARD"),
		// ContentLength:        aws.Int64(int64(file.Len())),

	})
	if err != nil {
		return "", err
	}
	return path, err
}

// Upload saves a file to aws bucket and returns the url to // the file and an error if there's any
func (s S3Service) UploadMultipart(file multipart.File, fileHeader *multipart.FileHeader, folder string, maxSize int64) (string, error) {
	// get the file size and read
	// the file content into a buffer
	size := fileHeader.Size
	if maxSize < fileHeader.Size {
		return "", errors.New("File size should be less than " + string(maxSize))
	}
	buffer := make([]byte, size)
	file.Read(buffer)
	uuid, _ := uuid.NewRandom()
	// create a unique file name for the file
	tempFileName := folder + "/" + uuid.String() + filepath.Ext(fileHeader.Filename)

	// config settings: this is where you choose the bucket,
	// filename, content-type and storage class of the file
	// you're uploading
	_, err := s3.New(s.connectToS3()).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(s.Bucket),
		Key:                  aws.String(tempFileName),
		ACL:                  aws.String("public-read"), // could be private if you want it to be access by only authorized users
		Body:                 bytes.NewReader(buffer),
		ContentLength:        aws.Int64(int64(size)),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
		StorageClass:         aws.String("STANDARD"),
	})
	if err != nil {
		return "", err
	}

	return tempFileName, err
}

// Delete deletes a file to aws bucket and returns  an error if there's any
func (s S3Service) Delete(path string) (bool, error) {
	// get path of the file

	// config settings: this is where you choose the bucket,
	//filepath of the object that needs to be deleted
	// you're deleting
	_, err := s3.New(s.connectToS3()).DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return false, err
	}

	return true, err
}

func (s S3Service) ListBucket() ([]string, error) {
	svc := s3.New(s.connectToS3())
	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(s.Bucket)})
	if err != nil {
		return nil, err
	}
	var files []string
	for _, key := range resp.Contents {
		files = append(files, *key.Key)
	}
	return files, nil
}
