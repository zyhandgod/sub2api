package service

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/httpclient"
)

// OpenAIProxyAutoSwitchService periodically tests active proxies against OpenAI
// and moves OpenAI accounts to the fastest acceptable proxy.
type OpenAIProxyAutoSwitchService struct {
	accountRepo       AccountRepository
	proxyRepo         ProxyRepository
	proxyProber       ProxyExitInfoProber
	proxyLatencyCache ProxyLatencyCache
	cfg               config.OpenAIProxyAutoSwitchConfig

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

type openAIProxyCandidate struct {
	proxy     Proxy
	latencyMs int64
	exitInfo  *ProxyExitInfo
	item      ProxyQualityCheckItem
}

func NewOpenAIProxyAutoSwitchService(
	accountRepo AccountRepository,
	proxyRepo ProxyRepository,
	proxyProber ProxyExitInfoProber,
	proxyLatencyCache ProxyLatencyCache,
	cfg config.OpenAIProxyAutoSwitchConfig,
) *OpenAIProxyAutoSwitchService {
	return &OpenAIProxyAutoSwitchService{
		accountRepo:       accountRepo,
		proxyRepo:         proxyRepo,
		proxyProber:       proxyProber,
		proxyLatencyCache: proxyLatencyCache,
		cfg:               normalizeOpenAIProxyAutoSwitchConfig(cfg),
		stopCh:            make(chan struct{}),
	}
}

func normalizeOpenAIProxyAutoSwitchConfig(cfg config.OpenAIProxyAutoSwitchConfig) config.OpenAIProxyAutoSwitchConfig {
	if cfg.IntervalMinutes <= 0 {
		cfg.IntervalMinutes = 60
	}
	if cfg.MaxLatencyMs <= 0 {
		cfg.MaxLatencyMs = 2000
	}
	if len(cfg.ExcludedCountryCodes) == 0 {
		cfg.ExcludedCountryCodes = []string{"HK"}
	}
	return cfg
}

func (s *OpenAIProxyAutoSwitchService) Start() {
	if s == nil || !s.cfg.Enabled || s.accountRepo == nil || s.proxyRepo == nil || s.proxyProber == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		interval := time.Duration(s.cfg.IntervalMinutes) * time.Minute
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		s.runOnce()
		for {
			select {
			case <-ticker.C:
				s.runOnce()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *OpenAIProxyAutoSwitchService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

func (s *OpenAIProxyAutoSwitchService) runOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	best, err := s.findBestProxy(ctx)
	if err != nil {
		log.Printf("[OpenAIProxyAutoSwitch] find best proxy failed: %v", err)
		return
	}
	if best == nil {
		log.Printf("[OpenAIProxyAutoSwitch] no OpenAI-capable proxy under %dms", s.cfg.MaxLatencyMs)
		return
	}

	changed, err := s.switchOpenAIAccounts(ctx, best.proxy.ID)
	if err != nil {
		log.Printf("[OpenAIProxyAutoSwitch] switch OpenAI accounts to proxy %d failed: %v", best.proxy.ID, err)
		return
	}
	if changed > 0 {
		log.Printf("[OpenAIProxyAutoSwitch] switched %d OpenAI accounts to proxy %d (%s), openai_latency=%dms",
			changed, best.proxy.ID, best.proxy.Name, best.latencyMs)
	}
}

func (s *OpenAIProxyAutoSwitchService) findBestProxy(ctx context.Context) (*openAIProxyCandidate, error) {
	proxies, err := s.proxyRepo.ListActive(ctx)
	if err != nil {
		return nil, err
	}

	var best *openAIProxyCandidate
	for i := range proxies {
		candidate := s.testProxyForOpenAI(ctx, proxies[i])
		if candidate == nil {
			continue
		}
		if best == nil || candidate.latencyMs < best.latencyMs {
			best = candidate
		}
	}
	return best, nil
}

func (s *OpenAIProxyAutoSwitchService) testProxyForOpenAI(ctx context.Context, proxy Proxy) *openAIProxyCandidate {
	proxyURL := proxy.URL()
	exitInfo, baseLatencyMs, err := s.proxyProber.ProbeProxy(ctx, proxyURL)
	if err != nil {
		s.saveProxyLatency(ctx, proxy.ID, &ProxyLatencyInfo{
			Success:   false,
			Message:   err.Error(),
			UpdatedAt: time.Now(),
		})
		return nil
	}

	if s.isExcludedExit(exitInfo) {
		s.saveOpenAIProxyQuality(ctx, proxy.ID, exitInfo, baseLatencyMs, "failed", 0, "F", "OpenAI 代理出口地区已排除")
		return nil
	}

	client, err := httpclient.GetClient(httpclient.Options{
		ProxyURL:              proxyURL,
		Timeout:               proxyQualityRequestTimeout,
		ResponseHeaderTimeout: proxyQualityResponseHeaderTimeout,
	})
	if err != nil {
		s.saveOpenAIProxyQuality(ctx, proxy.ID, exitInfo, baseLatencyMs, "failed", 0, "F", fmt.Sprintf("创建检测客户端失败: %v", err))
		return nil
	}

	item := runProxyQualityTarget(ctx, client, openAIProxyQualityTarget())
	if item.Status != "pass" {
		s.saveOpenAIProxyQuality(ctx, proxy.ID, exitInfo, baseLatencyMs, "failed", 0, "F", item.Message)
		return nil
	}

	latencyMs := item.LatencyMs
	if latencyMs <= 0 {
		latencyMs = baseLatencyMs
	}
	if latencyMs <= 0 {
		s.saveOpenAIProxyQuality(ctx, proxy.ID, exitInfo, baseLatencyMs, "failed", 0, "F", "OpenAI 延迟检测无效")
		return nil
	}
	if s.cfg.MaxLatencyMs > 0 && latencyMs > s.cfg.MaxLatencyMs {
		s.saveOpenAIProxyQuality(ctx, proxy.ID, exitInfo, baseLatencyMs, "warn", 70, "C",
			fmt.Sprintf("OpenAI 延迟 %dms 超过阈值 %dms", latencyMs, s.cfg.MaxLatencyMs))
		return nil
	}

	s.saveOpenAIProxyQuality(ctx, proxy.ID, exitInfo, latencyMs, "healthy", 100, "A",
		fmt.Sprintf("OpenAI 可达，延迟 %dms", latencyMs))
	return &openAIProxyCandidate{
		proxy:     proxy,
		latencyMs: latencyMs,
		exitInfo:  exitInfo,
		item:      item,
	}
}

func openAIProxyQualityTarget() proxyQualityTarget {
	for _, target := range proxyQualityTargets {
		if target.Target == "openai" {
			return target
		}
	}
	return proxyQualityTarget{
		Target: "openai",
		URL:    "https://api.openai.com/v1/models",
		Method: http.MethodGet,
		AllowedStatuses: map[int]struct{}{
			http.StatusUnauthorized: {},
		},
	}
}

func (s *OpenAIProxyAutoSwitchService) isExcludedExit(exitInfo *ProxyExitInfo) bool {
	if exitInfo == nil {
		return false
	}
	excluded := make(map[string]struct{}, len(s.cfg.ExcludedCountryCodes))
	for _, code := range s.cfg.ExcludedCountryCodes {
		code = strings.ToUpper(strings.TrimSpace(code))
		if code != "" {
			excluded[code] = struct{}{}
		}
	}
	if _, ok := excluded[strings.ToUpper(strings.TrimSpace(exitInfo.CountryCode))]; ok {
		return true
	}

	text := strings.ToLower(strings.Join([]string{exitInfo.Country, exitInfo.Region, exitInfo.City}, " "))
	return strings.Contains(text, "hong kong") || strings.Contains(text, "香港")
}

func (s *OpenAIProxyAutoSwitchService) switchOpenAIAccounts(ctx context.Context, proxyID int64) (int64, error) {
	accounts, err := s.accountRepo.ListActive(ctx)
	if err != nil {
		return 0, err
	}

	ids := make([]int64, 0, len(accounts))
	for i := range accounts {
		account := accounts[i]
		if !strings.EqualFold(strings.TrimSpace(account.Platform), PlatformOpenAI) {
			continue
		}
		if account.ProxyID != nil && *account.ProxyID == proxyID {
			continue
		}
		ids = append(ids, account.ID)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	targetProxyID := proxyID
	return s.accountRepo.BulkUpdate(ctx, ids, AccountBulkUpdate{ProxyID: &targetProxyID})
}

func (s *OpenAIProxyAutoSwitchService) saveOpenAIProxyQuality(
	ctx context.Context,
	proxyID int64,
	exitInfo *ProxyExitInfo,
	latencyMs int64,
	status string,
	score int,
	grade string,
	message string,
) {
	checkedAt := time.Now().Unix()
	latency := latencyMs
	info := &ProxyLatencyInfo{
		Success:          status == "healthy",
		LatencyMs:        &latency,
		Message:          message,
		QualityStatus:    status,
		QualityScore:     &score,
		QualityGrade:     grade,
		QualitySummary:   message,
		QualityCheckedAt: &checkedAt,
		UpdatedAt:        time.Now(),
	}
	if exitInfo != nil {
		info.IPAddress = exitInfo.IP
		info.Country = exitInfo.Country
		info.CountryCode = exitInfo.CountryCode
		info.Region = exitInfo.Region
		info.City = exitInfo.City
	}
	s.saveProxyLatency(ctx, proxyID, info)
}

func (s *OpenAIProxyAutoSwitchService) saveProxyLatency(ctx context.Context, proxyID int64, info *ProxyLatencyInfo) {
	if s.proxyLatencyCache == nil || info == nil {
		return
	}
	if err := s.proxyLatencyCache.SetProxyLatency(ctx, proxyID, info); err != nil {
		log.Printf("[OpenAIProxyAutoSwitch] store proxy latency failed: %v", err)
	}
}
