package server

type Google struct {
	OAuth2Provider
	Domains []string // Optional google email domain checking
}

/*
	client := conf.Client(oauth2.NoContext, tok)
	email, err := client.Get("https://www.googleapis.com/oauth2/v3/userinfo")
    if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
        return
	}
    defer email.Body.Close()
    data, _ := ioutil.ReadAll(email.Body)
    log.Println("Email body: ", string(data))
    c.Status(http.StatusOK)
*/
