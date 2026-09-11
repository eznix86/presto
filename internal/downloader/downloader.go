package downloader

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aras/presto/internal/resolver"
)

// Downloader handles parallel package downloads
type Downloader struct {
	workers    int
	httpClient *http.Client
	vendorDir  string
}

// NewDownloader creates a new downloader with specified number of workers
func NewDownloader(workers int) *Downloader {
	return &Downloader{
		workers: workers,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
		vendorDir: "vendor",
	}
}

// Progress is called as each package lands, with the number finished so far.
type Progress func(done, total int, name string)

// DownloadAll fetches every package in parallel and returns the ones that were
// not already in the vendor directory, sorted by name.
func (d *Downloader) DownloadAll(packages []*resolver.Package, progress Progress) ([]*resolver.Package, error) {
	if err := os.MkdirAll(d.vendorDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create vendor directory: %w", err)
	}

	jobs := make(chan *resolver.Package, len(packages))
	errs := make(chan error, len(packages))

	var (
		mu        sync.Mutex
		wg        sync.WaitGroup
		done      int
		installed []*resolver.Package
	)

	for i := 0; i < d.workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for pkg := range jobs {
				fetched, err := d.downloadPackage(pkg)
				if err != nil {
					errs <- fmt.Errorf("failed to download %s: %w", pkg.Name, err)
					continue
				}

				mu.Lock()
				done++
				if fetched {
					installed = append(installed, pkg)
				}
				if progress != nil {
					progress(done, len(packages), pkg.Name)
				}
				mu.Unlock()
			}
		}()
	}

	for _, pkg := range packages {
		jobs <- pkg
	}
	close(jobs)

	wg.Wait()
	close(errs)

	var downloadErrors []error
	for err := range errs {
		downloadErrors = append(downloadErrors, err)
	}

	if len(downloadErrors) > 0 {
		return nil, fmt.Errorf("download errors: %v", downloadErrors)
	}

	sort.Slice(installed, func(i, j int) bool {
		return installed[i].Name < installed[j].Name
	})

	return installed, nil
}

// downloadPackage reports whether it had to fetch the package.
func (d *Downloader) downloadPackage(pkg *resolver.Package) (bool, error) {
	packageDir := filepath.Join(d.vendorDir, pkg.Name)
	if _, err := os.Stat(packageDir); err == nil {
		return false, nil
	}

	resp, err := d.httpClient.Get(pkg.URL)
	if err != nil {
		return false, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", "presto-*.zip")
	if err != nil {
		return false, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return false, fmt.Errorf("download failed: %w", err)
	}

	// Close file to ensure everything is flushed to disk before extraction
	tmpFile.Close()

	if err := d.extractZip(tmpFile.Name(), packageDir); err != nil {
		return false, fmt.Errorf("extraction failed: %w", err)
	}

	return true, nil
}

// extractZip extracts a zip archive to the destination directory
func (d *Downloader) extractZip(zipPath, destDir string) error {
	// Open zip file
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Create destination directory
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	// Extract files
	for _, file := range reader.File {
		// Get the file path
		path := filepath.Join(destDir, file.Name)

		// Remove the first directory component (package name with version)
		parts := strings.Split(file.Name, string(filepath.Separator))
		if len(parts) > 1 {
			path = filepath.Join(destDir, filepath.Join(parts[1:]...))
		}

		// Check for directory
		if file.FileInfo().IsDir() {
			_ = os.MkdirAll(path, file.Mode())
			continue
		}

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}

		// Extract file
		if err := d.extractFile(file, path); err != nil {
			return err
		}
	}

	return nil
}

// extractFile extracts a single file from the zip archive
func (d *Downloader) extractFile(file *zip.File, destPath string) error {
	// Open file in archive
	srcFile, err := file.Open()
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Create destination file
	destFile, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
	if err != nil {
		return err
	}
	defer destFile.Close()

	// Copy contents
	if _, err := io.Copy(destFile, srcFile); err != nil {
		return err
	}

	return nil
}

// DownloadPackage reports whether it had to fetch the package.
func (d *Downloader) DownloadPackage(pkg *resolver.Package) (bool, error) {
	return d.downloadPackage(pkg)
}

// SetVendorDir sets the vendor directory path
func (d *Downloader) SetVendorDir(dir string) {
	d.vendorDir = dir
}
