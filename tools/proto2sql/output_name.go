package proto2sql

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidateOutputFileName 校验 name 在主流文件系统上都能安全地作为单个文件名使用。
//
// 校验刻意不依赖 runtime.GOOS：schema.sql 往往在 Linux CI 生成、提交后再被 Windows
// 开发机消费。若只按生成机器的规则检查，CON.sql、foo:bar.sql 等名称会一直到另一台
// 机器才失败；Users.sql / users.sql 的碰撞则由 Generate 的 table_name 去重统一拦截。
func ValidateOutputFileName(name string) error {
	if name == "" {
		return fmt.Errorf("不能为空")
	}
	if name == "." || name == ".." || strings.HasPrefix(name, "..") {
		return fmt.Errorf("不能是 . / .. 或以 .. 开头")
	}
	if name != filepath.Base(name) || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("不能含路径分隔符")
	}
	if filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
		return fmt.Errorf("不能是绝对路径")
	}
	if strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
		return fmt.Errorf("不能以空格或句点结尾")
	}

	for _, r := range name {
		if r < 0x20 {
			return fmt.Errorf("不能含 ASCII 控制字符 U+%04X", r)
		}
		if strings.ContainsRune(`<>:"/\|?*`, r) {
			return fmt.Errorf("不能含 Windows 非法字符 %q", r)
		}
	}
	if isWindowsReservedFileName(name) {
		return fmt.Errorf("不能使用 Windows 保留设备名")
	}
	return nil
}

// isWindowsReservedFileName 按 Win32 的规则检查 DOS 设备名；扩展名不会解除保留，
// 因而 CON、CON.sql、nul.archive.sql 都必须拒绝。
func isWindowsReservedFileName(name string) bool {
	name = strings.TrimRight(name, " .")
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		name = name[:dot]
	}
	name = strings.ToUpper(strings.TrimRight(name, " "))

	switch name {
	case "CON", "PRN", "AUX", "NUL", "CLOCK$", "CONIN$", "CONOUT$":
		return true
	case "COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³":
		// Windows 也把 ISO-8859-1 上标数字视为设备号。
		return true
	}
	if len(name) != 4 {
		return false
	}
	prefix, digit := name[:3], name[3]
	return (prefix == "COM" || prefix == "LPT") && digit >= '1' && digit <= '9'
}
