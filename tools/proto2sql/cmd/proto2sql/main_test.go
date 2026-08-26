package main

import (
	"strings"
	"testing"
)

// TestDropModeBannerContent -drop 的警告必须跟着生成物走。
//
// 光在 CLI 帮助和文档里写警告是不够的——真正的风险是**生成出来的 .sql 文件被别人
// 拿去执行**：它看起来就是一份普通的建表脚本，`mysql < schema.sql` 一敲，整库蒸发。
func TestDropModeBannerContent(t *testing.T) {
	for _, want := range []string{
		"DROP TABLE IF EXISTS",
		"切勿",
		"GenerateMigrationSQL", // 指出安全的替代路径
	} {
		if !strings.Contains(dropModeBanner, want) {
			t.Errorf("横幅缺少 %q:\n%s", want, dropModeBanner)
		}
	}
	if !strings.HasPrefix(dropModeBanner, "-- ") {
		t.Error("横幅必须整体是 SQL 注释，否则灌库时会语法错误")
	}
	for _, line := range strings.Split(strings.TrimRight(dropModeBanner, "\n"), "\n") {
		if !strings.HasPrefix(line, "--") {
			t.Errorf("横幅每一行都必须是 SQL 注释，这行不是: %q", line)
		}
	}
}
