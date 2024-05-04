package main

import (
	"context"

	"cloud.google.com/go/storage"
)

type ObjectStore interface {
	Upload(bucket string, prefix string, filename string, content []byte) error
}

type GoogleCloudStorage struct {
	client *storage.Client
}

func NewGoogleCloudStorage() (*GoogleCloudStorage, error) {
	// TODO: allow auth with explcit credentials or GKE service account
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return &GoogleCloudStorage{
		client: client,
	}, nil
}

func (u *GoogleCloudStorage) Upload(bucket string, prefix string, filename string, content []byte) error {
	ctx := context.Background()
	bkt := u.client.Bucket(bucket)
	obj := bkt.Object(prefix + "/" + filename)
	w := obj.NewWriter(ctx)
	defer w.Close()
	_, err := w.Write(content)

	return err
}

type FakeUploader struct {
	files map[string][]byte
}

func NewFakeUploader() *FakeUploader {
	return &FakeUploader{}
}

func (u *FakeUploader) Upload(bucket string, prefix string, filename string, content []byte) error {
	if u.files == nil {
		u.files = make(map[string][]byte)
	}

	u.files[filename] = content

	return nil
}

func (u *FakeUploader) Has(filename string) bool {
	if u.files == nil {
		return false
	}

	_, ok := u.files[filename]
	return ok
}
