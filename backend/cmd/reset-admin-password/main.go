// reset-admin-password 将指定邮箱的管理员密码更新为 bcrypt 哈希（与 Web 登录一致）。
//
// 用法示例（在 backend 目录）：
//
//	go run ./cmd/reset-admin-password -host 127.0.0.1 -dbpass '<POSTGRES_PASSWORD>' '新密码'
//
// 仅生成哈希（便于在容器内 psql 手工 UPDATE）：
//
//	go run ./cmd/reset-admin-password -hash-only '新密码'
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

func main() {
	var (
		email    = flag.String("email", "admin@sub2api.local", "管理员邮箱")
		hashOnly = flag.Bool("hash-only", false, "只输出 bcrypt 哈希，不写数据库")
		host     = flag.String("host", getenv("DATABASE_HOST", "127.0.0.1"), "PostgreSQL 主机")
		port     = flag.String("port", getenv("DATABASE_PORT", "5432"), "PostgreSQL 端口")
		dbUser   = flag.String("dbuser", firstNonEmpty(os.Getenv("DATABASE_USER"), os.Getenv("POSTGRES_USER"), "sub2api"), "数据库用户")
		dbPass   = flag.String("dbpass", firstNonEmpty(os.Getenv("DATABASE_PASSWORD"), os.Getenv("POSTGRES_PASSWORD"), ""), "数据库密码")
		dbname   = flag.String("dbname", firstNonEmpty(os.Getenv("DATABASE_DBNAME"), os.Getenv("POSTGRES_DB"), "sub2api"), "数据库名")
	)
	flag.Parse()
	plain := flag.Arg(0)
	if plain == "" {
		fmt.Fprintln(os.Stderr, "用法: go run ./cmd/reset-admin-password [选项] <新管理员密码>")
		flag.PrintDefaults()
		os.Exit(2)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	hs := string(hash)

	if *hashOnly {
		fmt.Println(hs)
		return
	}

	if *dbPass == "" {
		fmt.Fprintln(os.Stderr, "未设置数据库密码。请使用 -dbpass，或设置环境变量 DATABASE_PASSWORD / POSTGRES_PASSWORD；也可用 -hash-only 生成哈希后在 postgres 容器内执行 UPDATE。")
		os.Exit(1)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		*host, *port, *dbUser, *dbPass, *dbname)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "连接 PostgreSQL 失败:", err)
		os.Exit(1)
	}

	res, err := db.ExecContext(ctx,
		`UPDATE users SET password_hash = $1, updated_at = NOW() WHERE email = $2 AND role = $3 AND deleted_at IS NULL`,
		hs, *email, "admin")
	if err != nil {
		fmt.Fprintln(os.Stderr, "UPDATE 失败:", err)
		os.Exit(1)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		fmt.Fprintf(os.Stderr, "未更新任何行：请确认邮箱 %q 存在且 role=admin。\n", *email)
		os.Exit(1)
	}
	fmt.Printf("已将管理员 %q 的密码更新为新值（影响 %d 行）。\n", *email, n)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
