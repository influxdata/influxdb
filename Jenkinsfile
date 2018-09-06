properties([disableConcurrentBuilds()])

node("dind") {
    container('dind') {
        compat.test_build()
    }
}
