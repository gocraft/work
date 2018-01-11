package task

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/Guazi-inc/go-work"
	"github.com/go-errors/errors"
	"golang.guazi-corp.com/znkf/data-soup/http/staff"
)

/*******************************************************************************
 *  脚本名称：TasksDailyReport
 *  脚本功能：发送每日脚本运行报表
 *  执行计划：每天凌晨8点
 *  Author: panxiaohuai
 *******************************************************************************/

const (
	timeFormat = "2006-01-02 15:04:05"
	ns         = "__cron_report__"
)

// DailyReportTime Global variable, can be modified by application
var DailyReportTime = "08:00:00"
var Title = "Go服务后台任务执行情况报表"
var mailGroup map[string]bool
var redis CronCache
var staffHTTP *staff.HTTP

// TasksDailyReport job context
type TasksDailyReport struct {
	TaskBase
}

var regexMail = regexp.MustCompile(`^[A-Za-zd]+([-_.][A-Za-zd]+)*@([A-Za-zd]+[-.])+[A-Za-zd]{2,5}$`)

// Setup init func
func (t *TasksDailyReport) Setup() error {
	t.SetMetaData(t, ns, "panxiaohuai@guazi.com", "每日脚本执行报告", false)

	ts := work.NewSpec()
	ts.Daily(DailyReportTime)
	return t.AddCronTask("", ts, t.Exec)
}

// Exec worker func
func (t *TasksDailyReport) Exec(_ *work.Job) error {
	jobs, err := fetchMetrics()
	if err != nil {
		return err
	}
	if staffHTTP == nil {
		return errors.New("[go-work error] staffHttp nil")
	}

	return t.sendMail(jobs)
}

func (t *TasksDailyReport) sendMail(jobs map[string]*jobMetrics) error {
	body1 := fmt.Sprintf(`<h3>%s</h3>
				<table border="1" cellspacing="0">
					<tr height=30 bgColor=#00CD66>
						<td>脚本</td>
						<td>描述</td>
						<td>业务线</td>
						<td>Owner</td>
						<td>Schedule</td>
						<td>成功次数</td>
						<td>失败次数</td>
						<td>平均耗时(秒)</td>
						<td>最后报错信息</td>
					</tr>`, Title)
	for _, v := range jobs {
		elapsed := 0
		succ, _ := strconv.Atoi(v.succ)
		if succ > 0 {
			total, _ := strconv.Atoi(v.elapsed)
			elapsed = total / succ
		}

		body1 += fmt.Sprintf("<tr height=30><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%s</td></tr>",
			v.name,
			v.desc,
			v.ns,
			v.owner,
			v.schedule,
			v.succ,
			v.fail,
			elapsed/1000,
			v.lastErr,
		)

	}
	body1 += "</table>"

	timeNow := time.Now()
	body := `<!DOCTYPE html><html><head></head><body>` +
		fmt.Sprintf("统计时间: %s - %s", timeNow.Add(-time.Hour*24).Format(timeFormat), timeNow.Format(timeFormat)) +
		body1 +
		"</body></html>"

	url := "contact/sendMail"
	subject := fmt.Sprintf("%s %d-%d-%d", Title, timeNow.Year(), timeNow.Month(), timeNow.Day())
	var receivers string
	for r := range mailGroup {
		receivers += "," + r
	}

	if len(receivers) == 0 {
		return nil
	}

	params := map[string]string{
		"to":      receivers,
		"subject": subject,
		"body":    body,
	}

	var resp interface{}
	err := staffHTTP.PostMultipart(url, params, &resp, map[string]string{})
	if err != nil {
		panic(err)
	}

	return nil
}

func AddMailReceiver(receiver string) {
	if mailGroup == nil {
		mailGroup = map[string]bool{}
	}
	if regexMail.Match([]byte(receiver)) {
		mailGroup[receiver] = true
	}
}

func SetCache(c CronCache) {
	redis = c
}

func SetStaffClient(s *staff.HTTP) {
	staffHTTP = s
}
