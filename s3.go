package s3service

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3Service - s3 service
type S3Service struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Token           string
	Bucket          string
}

// connectToS3 - connect to s3
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

// DownloadFile - download file from s3 to temp folder
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

// Upload saves a file to aws bucket with multipart upload and returns the url to // the file and an error if there's any
func (s S3Service) UploadAsMultipart(file *bytes.Buffer, path string) (string, error) {
	// get length of file in bytes
	lengthFile := file.Len()
	fmt.Println("lengthFile", lengthFile)

	buffer := make([]byte, file.Len())
	file.Read(buffer)
	// get the file size in bytes

	// Create a multipart upload request
	req, _ := s3.New(s.connectToS3()).CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:               aws.String(s.Bucket),
		Key:                  aws.String(path),
		ACL:                  aws.String("public-read"), // could be private if you want it to be access by only authorized users
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
		StorageClass:         aws.String("STANDARD"),

		// ContentLength:        aws.Int64(int64(file.Len())),
	})

	// Split the file into 5MB parts, caring about the last part being smaller than 5MB
	parts := make([][]byte, 0)
	for i := 0; i < lengthFile; i += 5 * 1024 * 1024 {
		end := i + 5*1024*1024
		if end > lengthFile {
			end = lengthFile
		}
		parts = append(parts, buffer[i:end])
	}

	bytesRead := 0
	var completed_parts []*s3.CompletedPart

	index := 0
	for bytesRead < lengthFile {
		fmt.Println("index, bytesRead, lengthFile, len(parts[index])", index, bytesRead, lengthFile, len(parts[index]))
		bytesRead += len(parts[index])
		// Upload a part
		result, err := s3.New(s.connectToS3()).UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(s.Bucket),
			Key:        aws.String(path),
			PartNumber: aws.Int64(int64(len(completed_parts) + 1)),
			UploadId:   req.UploadId,
			Body:       bytes.NewReader(parts[index]),
		})
		index += 1
		if err != nil {
			return "", err
		}
		completed_parts = append(completed_parts, &s3.CompletedPart{
			ETag:       result.ETag,
			PartNumber: aws.Int64(int64(len(completed_parts) + 1)),
		})
	}
	fmt.Println("END")

	// Complete the multipart upload
	_, err := s3.New(s.connectToS3()).CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.Bucket),
		Key:      aws.String(path),
		UploadId: req.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completed_parts,
		},
	})
	if err != nil {
		return "", err
	}

	return path, err
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

// ListBucket lists all the files in a bucket
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

// CheckObjectExists checks if an object exists in a bucket
func (s S3Service) CheckObjectExists(path string) (bool, error) {
	svc := s3.New(s.connectToS3())
	_, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return false, err
	}
	return true, nil
}
