package storage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/mcqueenorama/weed-fs/go/glog"
	"github.com/mcqueenorama/weed-fs/go/images"
	"github.com/mcqueenorama/weed-fs/go/util"
)

const (
	NeedleHeaderSize      = 16 //should never change this
	NeedlePaddingSize     = 8
	NeedleChecksumSize    = 4
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Cookie uint32 `comment:"random number to mitigate brute force lookups"`
	Id     uint64 `comment:"needle id"`
	Size   uint32 `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`

	DataSize     uint32 `comment:"Data size"` //version2
	Data         []byte `comment:"The actual file data"`
	Flags        byte   `comment:"boolean flags"` //version2
	NameSize     uint8  //version2
	Name         []byte `comment:"maximum 256 characters"` //version2
	MimeSize     uint8  //version2
	Mime         []byte `comment:"maximum 256 characters"` //version2
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk
	Ttl          *TTL

	Checksum CRC    `comment:"CRC32 to check integrity"`
	Padding  []byte `comment:"Aligned to 8 bytes"`
}

func (n *Needle) String() (str string) {
	str = fmt.Sprintf("Cookie:%d, Id:%d, Size:%d, DataSize:%d, Name: %s, Mime: %s", n.Cookie, n.Id, n.Size, n.DataSize, n.Name, n.Mime)
	return
}

func ParseUpload(r *http.Request) (fileName string, data []byte, mimeType string, isGzipped bool, modifiedTime uint64, ttl *TTL, e error) {
	form, fe := r.MultipartReader()
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}
	part, fe := form.NextPart()
	if fe != nil {
		glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}
	fileName = part.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}

	data, e = ioutil.ReadAll(part)
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}
	dotIndex := strings.LastIndex(fileName, ".")
	ext, mtype := "", ""
	if dotIndex > 0 {
		ext = strings.ToLower(fileName[dotIndex:])
		mtype = mime.TypeByExtension(ext)
	}
	contentType := part.Header.Get("Content-Type")
	if contentType != "" && mtype != contentType {
		mimeType = contentType //only return mime type if not deductable
		mtype = contentType
	}
	if part.Header.Get("Content-Encoding") == "gzip" {
		isGzipped = true
	} else if IsGzippable(ext, mtype) {
		if data, e = GzipData(data); e != nil {
			return
		}
		isGzipped = true
	}
	if ext == ".gz" {
		isGzipped = true
	}
	if strings.HasSuffix(fileName, ".gz") {
		fileName = fileName[:len(fileName)-3]
	}
	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	ttl, _ = ReadTTL(r.FormValue("ttl"))
	return
}
func NewNeedle(r *http.Request, fixJpgOrientation bool) (n *Needle, e error) {
	fname, mimeType, isGzipped := "", "", false
	n = new(Needle)
	fname, n.Data, mimeType, isGzipped, n.LastModified, n.Ttl, e = ParseUpload(r)
	if e != nil {
		return
	}
	if len(fname) < 256 {
		n.Name = []byte(fname)
		n.SetHasName()
	}
	if len(mimeType) < 256 {
		n.Mime = []byte(mimeType)
		n.SetHasMime()
	}
	if isGzipped {
		n.SetGzipped()
	}
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()
	if n.Ttl != EMPTY_TTL {
		n.SetHasTtl()
	}

	if fixJpgOrientation {
		loweredName := strings.ToLower(fname)
		if mimeType == "image/jpeg" || strings.HasSuffix(loweredName, ".jpg") || strings.HasSuffix(loweredName, ".jpeg") {
			n.Data = images.FixJpgOrientation(n.Data)
		}
	}

	n.Checksum = NewCRC(n.Data)

	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	e = n.ParsePath(fid)

	return
}
func (n *Needle) ParsePath(fid string) (err error) {
	length := len(fid)
	if length <= 8 {
		return errors.New("Invalid fid:" + fid)
	}
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	n.Id, n.Cookie, err = ParseKeyHash(fid)
	if err != nil {
		return err
	}
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Id += d
		} else {
			return e
		}
	}
	return err
}

func ParseKeyHash(key_hash_string string) (uint64, uint32, error) {
	key_hash_bytes, khe := hex.DecodeString(key_hash_string)
	key_hash_len := len(key_hash_bytes)
	if khe != nil || key_hash_len <= 4 {
		glog.V(0).Infoln("Invalid key_hash", key_hash_string, "length:", key_hash_len, "error", khe)
		return 0, 0, errors.New("Invalid key and hash:" + key_hash_string)
	}
	key := util.BytesToUint64(key_hash_bytes[0 : key_hash_len-4])
	hash := util.BytesToUint32(key_hash_bytes[key_hash_len-4 : key_hash_len])
	return key, hash, nil
}
