package google_auth

import (
	"bytes"
	"encoding/json"

	"github.com/daemonl/go_gsd/shared"
	"io/ioutil"
	"log"
	"net/http"
)

type OAuthConfig struct {
	AuthUri                 string `json:"auth_uri"`
	ClientSecret            string `json:"client_secret"`
	TokenUri                string `json:"token_uri"`
	ClientEmail             string `json:"client_email"`
	ClientX509CertUrl       string `json:"client_x509_cert_url"`
	ClientId                string `json:"client_id"`
	AuthProviderx509CertUrl string `json:"auth_provider_x509_cert_url"`
	Key                     string `json:"key"`
	Identifier              string `json:"identifier"`
	ContinueUrl             string `json:"continue_url"`
	OpenidRealm             string `json:"openid_realm"`
}
type OAuthHandler struct {
	Config      *OAuthConfig
	LoginLogout shared.ILoginLogout
}

type createAuthUrlRequest struct {
	Identifier       string `json:"identifier"`
	ContinueUrl      string `json:"continueUrl"`
	OpenidRealm      string `json:"openidRealm"`
	OauthConsumerKey string `json:"oauthConsumerKey"`
	OauthScope       string `json:"oauthScope"`
	UiMode           string `json:"uiMode"`
	Context          string `json:"context"`
}

type createAuthUrlResponse struct {
	Kind    string `json:"kind"`
	AuthUri string `json:"authUri"`
}

type oauthVerifyResponse struct {
	Kind              string `json:"kind"`
	Identifier        string `json:"identifier"`
	Authority         string `json:"authority"`
	VerifiedEmail     string `json:"verifiedEmail"`
	Email             string `json:"email"`
	DisplayName       string `json:"displayName"`
	FirstName         string `json:"firstName"`
	LastName          string `json:"lastName"`
	FullName          string `json:"fullName"`
	NickName          string `json:"nickName"`
	Language          string `json:"language"`
	TimeZone          string `json:"timeZone"`
	ProfilePicture    string `json:"profilePicture"`
	PhotoUrl          string `json:"photoUrl"`
	DateOfBirth       string `json:"dateOfBirth"`
	OauthScope        string `json:"oauthScope"`
	OauthRequestToken string `json:"oauthRequestToken"`
	OauthAccessToken  string `json:"oauthAccessToken"`
	OauthExpireIn     uint64 `json:"oauthExpireIn"`
	OauthRefreshToken string `json:"oauthRefreshToken"`
	Context           string `json:"context"`
}

type oauthVerifyRequest struct {
	RequestURI       string `json:"requestUri"`
	ReturnOauthToken bool   `json:"returnOauthToken"`
}

func (oa *OAuthHandler) OauthResponse(request shared.IRequest) {

	_, r := request.GetRaw()

	endpoint := "https://www.googleapis.com/identitytoolkit/v1/relyingparty/verifyAssertion?key=" + oa.Config.Key

	requestUri := oa.Config.ContinueUrl + "?" + r.URL.RawQuery
	vr := oauthVerifyRequest{
		RequestURI: requestUri,
		//PostBody:         "",
		ReturnOauthToken: true,
	}
	body, err := json.Marshal(vr)
	bodyReader := bytes.NewReader(body)
	resp, err := http.Post(endpoint, "application/json", bodyReader)
	if err != nil {
		log.Println(err)
		request.Session().AddFlash("error", "An error occurred when communicating with the Google server")
		request.Redirect("/login")
		return
	}

	if resp.StatusCode != 200 {
		log.Println(resp)
		bs, _ := ioutil.ReadAll(resp.Body)
		log.Println(string(bs))
		request.Session().AddFlash("error", "An error occurred when communicating with the Google server")
		request.Redirect("/login")
		return
	}

	authResp := oauthVerifyResponse{}
	unm := json.NewDecoder(resp.Body)
	unm.Decode(&authResp)
	log.Printf("OAUTH RESP: %#v\n", authResp)
	oa.LoginLogout.ForceLogin(request, authResp.VerifiedEmail)
}
func (oa *OAuthHandler) OauthRequest(request shared.IRequest) {

	endpoint := "https://www.googleapis.com/identitytoolkit/v1/relyingparty/createAuthUrl?key=" + oa.Config.Key

	req := createAuthUrlRequest{
		Identifier:       oa.Config.Identifier,
		ContinueUrl:      oa.Config.ContinueUrl,
		OpenidRealm:      oa.Config.OpenidRealm,
		OauthConsumerKey: "",
		OauthScope:       "",
		UiMode:           "redirect",
		Context:          "1",
	}

	body, err := json.Marshal(req)
	bodyReader := bytes.NewReader(body)
	resp, err := http.Post(endpoint, "application/json", bodyReader)
	if err != nil {
		log.Println(err)
		request.Session().AddFlash("error", "An error occurred when communicating with the Google server")
		request.Redirect("/login")
		return
	}

	if resp.StatusCode != 200 {
		log.Println(resp.StatusCode)
		request.Session().AddFlash("error", "An error occurred when communicating with the Google server")
		request.Redirect("/login")
		return
	}

	authResp := createAuthUrlResponse{}
	unm := json.NewDecoder(resp.Body)
	unm.Decode(&authResp)
	request.Redirect(authResp.AuthUri)
	return
}
