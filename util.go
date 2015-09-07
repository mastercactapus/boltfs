package boltfs

type BucketPath [][]byte

func NewBucketPath(parts ...[]byte) BucketPath {
	return BucketPath(parts)
}

func (p BucketPath) Join(parts ...[]byte) BucketPath {
	newPath := make(BucketPath, len(p)+len(parts))
	copy(newPath, p)
	copy(newPath[len(p):], parts)
	return newPath
}

func (p BucketPath) BucketFrom(b Bucketer) Bucket {
	var bk Bucket
	for _, key := range p {
		if bk == nil {
			bk = b.Bucket(key)
		} else {
			bk = bk.Bucket(key)
		}
		if bk == nil {
			break
		}
	}
	return bk
}

func (p BucketPath) DeleteFrom(b Bucketer) error {
	bk := BucketPath(p[:len(p)-1]).BucketFrom(b)
	if bk == nil {
		return nil
	}
	return bk.DeleteBucket(p[len(p)-1])
}

func (p BucketPath) CreateFrom(b Bucketer) (Bucket, error) {
	bk, err := BucketPath(p[:len(p)-1]).MkFrom(b)
	if err != nil {
		return nil, err
	}
	return bk.CreateBucket(p[len(p)-1])
}

func (p BucketPath) MkFrom(b Bucketer) (Bucket, error) {
	var bk Bucket
	var err error
	for _, key := range p {
		if bk == nil {
			bk, err = b.CreateBucketIfNotExists(key)
		} else {
			bk, err = bk.CreateBucketIfNotExists(key)
		}
		if err != nil {
			return nil, err
		}
	}
	return bk, nil
}
