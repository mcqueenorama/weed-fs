package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/mcqueenorama/weed-fs/go/operation"
	"github.com/mcqueenorama/weed-fs/go/util"
)

var (
	downloadReplication *string
	downloadDir         *string
)

func init() {
	cmdDownload.Run = runDownload // break init cycle
	cmdDownload.IsDebug = cmdDownload.Flag.Bool("debug", false, "verbose debug information")
	server = cmdDownload.Flag.String("server", "localhost:9333", "weedfs master location")
	downloadDir = cmdDownload.Flag.String("dir", ".", "Download the whole folder recursively if specified.")
}

var cmdDownload = &Command{
	UsageLine: "download -server=localhost:9333 -dir=one_directory fid1 [fid2 fid3 ...]",
	Short:     "download files by file id",
	Long: `download files by file id.
  
  Usually you just need to use curl to lookup the file's volume server, and then download them directly.
  This download tool combine the two steps into one.

  What's more, if you use "weed upload -maxMB=..." option to upload a big file divided into chunks, you can
  use this tool to download the chunks and merge them automatically.

  `,
}

func runDownload(cmd *Command, args []string) bool {
	for _, fid := range args {
		filename, content, e := fetchFileId(*server, fid)
		if e != nil {
			fmt.Println("Fetch Error:", e)
			continue
		}
		if filename == "" {
			filename = fid
		}
		if strings.HasSuffix(filename, "-list") {
			filename = filename[0 : len(filename)-len("-list")]
			fids := strings.Split(string(content), "\n")
			f, err := os.OpenFile(path.Join(*downloadDir, filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
			if err != nil {
				fmt.Println("File Creation Error:", e)
				continue
			}
			defer f.Close()
			for _, partId := range fids {
				var n int
				_, part, err := fetchFileId(*server, partId)
				if err == nil {
					n, err = f.Write(part)
				}
				if err == nil && n < len(part) {
					err = io.ErrShortWrite
				}
				if err != nil {
					fmt.Println("File Write Error:", err)
					break
				}
			}
		} else {
			ioutil.WriteFile(path.Join(*downloadDir, filename), content, os.ModePerm)
		}
	}
	return true
}

func fetchFileId(server string, fileId string) (filename string, content []byte, e error) {
	fileUrl, lookupError := operation.LookupFileId(server, fileId)
	if lookupError != nil {
		return "", nil, lookupError
	}
	filename, content, e = util.DownloadUrl(fileUrl)
	return
}

func WriteFile(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	return err
}
