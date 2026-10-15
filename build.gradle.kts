plugins {
    java
    application
}

group = "io.github.keemgdeok"
version = "0.1.0-SNAPSHOT"

repositories { mavenCentral() }
java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }

val lab = sourceSets.create("lab")
dependencies {
    implementation("org.apache.kafka:kafka-clients:4.3.1")
    implementation("io.kubernetes:client-java:26.0.0")
    implementation("info.picocli:picocli:4.7.7")
    implementation("com.google.code.gson:gson:2.13.2")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    add(lab.implementationConfigurationName, "org.apache.kafka:kafka-clients:4.3.1")
    add(lab.implementationConfigurationName, "com.google.code.gson:gson:2.13.2")
    add(lab.runtimeOnlyConfigurationName, "org.slf4j:slf4j-simple:2.0.17")
}

application { mainClass = "io.github.keemgdeok.kafk8s.Main" }
tasks.test { useJUnitPlatform() }
tasks.processResources {
    inputs.property("version", project.version)
    filesMatching("version.properties") { expand("version" to project.version) }
}
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror")) }
tasks.register<Copy>("labBundle") {
    dependsOn(lab.classesTaskName)
    from(lab.output)
    into(layout.buildDirectory.dir("lab/classes"))
    from(configurations[lab.runtimeClasspathConfigurationName]) { into("../lib") }
}
