package db

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can factorize and defactorize values.
func TestFactorizer(t *testing.T) {
	withFactorizer(func(f *factorizer) {
		num, err := f.Factorize("foo", "bar", "/index.html", true)
		if err != nil || num != 1 {
			t.Fatalf("Wrong factorization: exp: %v, got: %v (%v)", 1, num, err)
		}
		num, err = f.Factorize("foo", "bar", "/about.html", true)
		if err != nil || num != 2 {
			t.Fatalf("Wrong factorization: exp: %v, got: %v (%v)", 2, num, err)
		}

		str, err := f.Defactorize("foo", "bar", 1)
		if err != nil || str != "/index.html" {
			t.Fatalf("Wrong defactorization: exp: %v, got: %v (%v)", "/index.html", str, err)
		}
		str, err = f.Defactorize("foo", "bar", 2)
		if err != nil || str != "/about.html" {
			t.Fatalf("Wrong defactorization: exp: %v, got: %v (%v)", "/about.html", str, err)
		}
	})
}

// Ensure that very large factorized values get truncated.
func TestFactorizerTruncate(t *testing.T) {
	withFactorizer(func(f *factorizer) {
		value := "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
		shortValue := f.truncate("foo", value)
		num, err := f.Factorize("foo", "bar", value, true)
		assert.Equal(t, num, uint64(1))
		assert.NoError(t, err)
		str, err := f.Defactorize("foo", "bar", 1)
		assert.Equal(t, len(str), 494)
		assert.NoError(t, err)
		num2, err := f.Factorize("foo", "bar", shortValue, true)
		assert.Equal(t, num2, uint64(1))
		assert.NoError(t, err)
	})
}

func withFactorizer(fn func(f *factorizer)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	f := NewFactorizer(path, false, 4096, 126).(*factorizer)
	if err := f.Open(); err != nil {
		panic(err.Error())
	}
	defer f.Close()

	fn(f)
}
