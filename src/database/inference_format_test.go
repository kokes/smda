package database

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"testing"
)

func TestBasicCompressionInference(t *testing.T) {
	file := []byte("a,b,c\n1,2,3")
	ctype := inferCompression(file)
	if ctype != compressionNone {
		t.Error("expecting an uncompressed file to be recognised as such")
		return
	}
}

func TestGzippedFile(t *testing.T) {
	file := new(bytes.Buffer)
	gf := gzip.NewWriter(file)
	if _, err := io.WriteString(gf, "a,b,c\n1,2,3"); err != nil {
		t.Error(err)
		return
	}
	gf.Flush()
	ctype := inferCompression(file.Bytes())
	if ctype != compressionGzip {
		t.Error("expecting a gzip file to be recognised as such")
		return
	}
}

// go doesn't have a bzip2 writer, only a reader
// so I wrote this python script to generate some bzip data
// import bz2
// import io

// buffer = io.BytesIO()
// to_write = 'a,b,c\n1,2,3'
// with bz2.open(buffer, 'wb') as f:
//     f.write(to_write.encode())

// buffer.seek(0)
// data = buffer.read().hex()

// for j in range(len(data)//2):
//     print(data[2*j:2*j+2])
func TestBzippedFile(t *testing.T) {
	buf := []byte{0x42, 0x5a, 0x68, 0x39, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59, 0xc9, 0x4b, 0x05, 0x83, 0x00, 0x00, 0x04, 0x59, 0x00, 0x00, 0x10, 0x00, 0x04, 0x38, 0x00, 0x38, 0x00, 0x20, 0x00, 0x21, 0xa0, 0x66, 0xa1, 0x0c, 0x08, 0x37, 0xa0, 0xe2, 0x2d, 0x78, 0xbb, 0x92, 0x29, 0xc2, 0x84, 0x86, 0x4a, 0x58, 0x2c, 0x18}
	ctype := inferCompression(buf)
	if ctype != compressionBzip2 {
		t.Error("expecting a bzip2 file to be recognised as such")
		return
	}
}

func TestInferDelimiterBasic(t *testing.T) {
	tests := []struct {
		firstLine         string
		expectedDelimiter delimiter
	}{
		// there's nothing we can do for single-column datasets
		{"hello", delimiterNone},
		{"hello\n", delimiterNone},
		{"hello\nworld\nhi\n", delimiterNone},

		{"hello,world", delimiterComma},
		{"hello,;world", delimiterComma}, // there is ordering to deal with ties
		{"hello;,world", delimiterComma},
		{"hello;world", delimiterSemicolon},
		{"hello\tworld", delimiterTab},
		{"hello|world", delimiterPipe},

		{"hello,\"world;other;things\"\n1,2\n", delimiterComma},
		{"\"my first column\",\"my second column\"\nsome,data\n", delimiterComma},
		{"\"my first column\";\"my second column\"\nsome;data\n", delimiterSemicolon},

		{"hello,world\n", delimiterComma},

		{"", delimiterNone},
		{"foo\n", delimiterNone},
	}

	for _, testCase := range tests {
		r := []byte(testCase.firstLine)
		inferredDelimiter := inferDelimiter(r)
		if inferredDelimiter != testCase.expectedDelimiter {
			t.Errorf("inferring delimiters, expected %+v, got %+v", testCase.expectedDelimiter, inferredDelimiter)
			return
		}
	}
}

func TestCompressionStringer(t *testing.T) {
	tests := []struct {
		cmp compression
		str string
	}{
		{compressionNone, "none"},
		{compressionGzip, "gzip"},
		{compressionBzip2, "bzip2"},
	}
	for _, test := range tests {
		if test.cmp.String() != test.str {
			t.Errorf("expecting %+v to print as %+v", test.cmp, test.str)
		}
	}
}

func TestDelimiterStringer(t *testing.T) {
	tests := []struct {
		dlm delimiter
		str string
	}{
		{delimiterNone, "none"},
		{delimiterComma, "comma"},
		{delimiterTab, "tab"},
		{delimiterPipe, "pipe"},
	}
	for _, test := range tests {
		if test.dlm.String() != test.str {
			t.Errorf("expecting %+v to print as %+v", test.dlm, test.str)
		}
	}
}

func TestWrappingUncompressedData(t *testing.T) {
	raw := []byte("foobarbaz")
	data := bytes.NewReader(raw)
	newReader, err := wrapCompressed(data, compressionNone)
	if err != nil {
		t.Fatal(err)
	}

	newData, err := ioutil.ReadAll(newReader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, newData) {
		t.Fatalf("expected %+v, got %+v", raw, newData)
	}
}

func TestWrappingGzippedData(t *testing.T) {
	raw := []byte("foobarbaz")
	gdata := new(bytes.Buffer)
	gw := gzip.NewWriter(gdata)
	if _, err := io.Copy(gw, bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	// gw.Flush()
	gw.Close()

	data := bytes.NewReader(gdata.Bytes())
	newReader, err := wrapCompressed(data, compressionGzip)
	if err != nil {
		t.Fatal(err)
	}

	newData, err := ioutil.ReadAll(newReader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, newData) {
		t.Fatalf("expected %+v, got %+v", raw, newData)
	}
}
