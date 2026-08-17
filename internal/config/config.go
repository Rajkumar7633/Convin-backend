// Package config loads service configuration from the environment.
package config

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config holds everything the service needs to start.
type Config struct {
	HTTPAddr    string
	PostgresDSN string
	RedisAddr   string
	// DBMaxConns bounds the Postgres connection pool.
	DBMaxConns int32
}

func init() {
	loadDotEnv()
}

func loadDotEnv() {
	// Search up to 3 levels up for .env
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	for i := 0; i < 4; i++ {
		envPath := filepath.Join(dir, ".env")
		if f, err := os.Open(envPath); err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				parts := strings.SplitN(line, "=", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					val := strings.TrimSpace(parts[1])
					val = strings.Trim(val, `"'`)
					if _, exists := os.LookupEnv(key); !exists {
						_ = os.Setenv(key, val)
					}
				}
			}
			_ = f.Close()
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
}

// Load reads configuration from the environment, falling back to the
// defaults used by docker-compose.yml.
func Load() Config {
	return Config{
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		PostgresDSN: env("DATABASE_URL", "postgres://webhook:webhook@localhost:5432/webhook?sslmode=disable"),
		RedisAddr:   env("REDIS_ADDR", "localhost:6379"),
		DBMaxConns:  int32(envInt("DB_MAX_CONNS", 20)),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}

