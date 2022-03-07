
kotlin.sourceSets {
    val jvmMain by getting {
        dependencies {
            api(project(":ktor-utils"))
            (libs.gson)
        }
    }
}
