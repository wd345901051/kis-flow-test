package metrics

import (
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// kisMetrics kisFlow的Prometheus监控指标
type kisMetrics struct {
	//数据数量总量
	DataTotal prometheus.Counter
}

var Metrics *kisMetrics

func RunMetricsService(serverAddr string) error {
	http.Handle(common.METRICS_ROUTE, promhttp.Handler())

	err := http.ListenAndServe(serverAddr, nil)
	if err != nil {
		log.Logger().ErrorF("RunMetricsService err = %s\n", err)
	}
	return err

}

func InitMetrics() {
	Metrics = new(kisMetrics)

	// DataTotal初始化Counter
	Metrics.DataTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: common.COUNTER_KISFLOW_DATA_TOTAL_NAME,
		Help: common.COUNTER_KISFLOW_DATA_TOTAL_HELP,
	})

	// 注册Metrics
	prometheus.MustRegister(Metrics.DataTotal)
}

func RunMetrics() {
	InitMetrics()
	if config.GlobalConfig.EnableProm == true && config.GlobalConfig.PrometheusListen == true {
		go RunMetricsService(config.GlobalConfig.PrometheusServe)
	}
}
