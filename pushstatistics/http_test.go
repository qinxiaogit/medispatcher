package pushstatistics

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/bouk/monkey"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testShowData = map[string]map[string]*Categorys{
		"v60": map[string]*Categorys{
			".1": &Categorys{
				Second: 1,
				Minute: 60,
				Hour:   3600,
				Day:    3600 * 24,
			},
			".2": &Categorys{
				Second: 1 * 2,
				Minute: 60 * 2,
				Hour:   3600 * 2,
				Day:    3600 * 24 * 2,
			},
		},
		"v1": map[string]*Categorys{
			".1": &Categorys{
				Second: 1,
				Minute: 2,
				Hour:   3,
				Day:    4 * 24,
			},
			".2": &Categorys{
				Second: 1 * 2,
				Minute: 2 * 2,
				Hour:   3 * 2,
				Day:    4 * 24 * 2,
			},
		},
	}
)

func TestGetPushStatistics(t *testing.T) {
	Convey("GetPushStatistics", t, func() {
		Convey("Get Data", func() {
			guard := Patch(ShowData, func() map[string]map[string]*Categorys {
				return testShowData
			})
			defer guard.Unpatch()

			req, e := http.NewRequest(http.MethodGet, "/prometeus/pushstatistics", nil)
			So(e, ShouldBeNil)
			resp := httptest.NewRecorder()

			GetPushStatistics(resp, req)

			So(resp.Code, ShouldEqual, http.StatusOK)
			defer resp.Result().Body.Close()
			data, e := ioutil.ReadAll(resp.Result().Body)
			So(e, ShouldBeNil)
			out := strings.Split(string(data), "\n")
			So(len(out), ShouldEqual, 9)
			So(len(out[8]), ShouldEqual, 0)
			out = out[:8]

			for index, item := range out {
				if index == 0 || index == 1 || index == 4 || index == 5 {
					So(strings.HasPrefix(item, "# "), ShouldBeTrue)
				} else {
					So(item, ShouldContainSubstring, "{")
					So(item, ShouldContainSubstring, "}")
				}
			}

		})
		Convey("Post Data", func() {
			guard := Patch(ShowData, func() map[string]map[string]*Categorys {
				return testShowData
			})
			defer guard.Unpatch()

			req, e := http.NewRequest(http.MethodPost, "/prometeus/pushstatistics", nil)
			So(e, ShouldBeNil)
			resp := httptest.NewRecorder()

			GetPushStatistics(resp, req)

			So(resp.Code, ShouldEqual, http.StatusMethodNotAllowed)
			defer resp.Result().Body.Close()
			data, e := ioutil.ReadAll(resp.Result().Body)
			So(e, ShouldBeNil)
			So(string(data), ShouldEqual, http.StatusText(http.StatusMethodNotAllowed))
		})
	})
}
