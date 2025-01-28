allprojects {
    val versionFromProperty = "${project.property("version")}"
    val versionFromEnv: String? = System.getenv("VERSION")

    version = versionFromEnv ?: versionFromProperty
    group = "${project.property("group")}"

    repositories {
        mavenCentral()
        maven(url = "https://nexus.sibmaks.ru/repository/maven-snapshots/")
        maven(url = "https://nexus.sibmaks.ru/repository/maven-releases/")
    }

}