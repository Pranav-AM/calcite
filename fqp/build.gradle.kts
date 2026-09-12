/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.gradle.api.tasks.testing.Test

dependencies {
    api(project(":core"))

    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("org.apache.arrow:arrow-memory-core:18.2.0")
    implementation("org.apache.arrow:flight-core:18.2.0")
    runtimeOnly("org.apache.arrow:arrow-memory-netty:18.2.0")
    implementation("org.apache.arrow:arrow-vector:18.2.0")
    // Generated Substrait protobuf bindings; the serializer intentionally
    // avoids the Java-11-only Calcite adapter from Substrait Java.
    implementation("io.substrait:core:0.62.0")
    testImplementation(project(":plus"))
    testImplementation(project(":testkit"))
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
}

tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}

tasks.named<Test>("test") {
    useJUnitPlatform { excludeTags("flightIntegration", "tpchIntegration") }
}

val workerDirectory = rootProject.layout.projectDirectory.dir("fqp-datafusion-worker")
val workerExecutable = providers.gradleProperty("fqpWorkerBinary").orElse(
    workerDirectory.file("target/debug/fqp-datafusion-worker" +
        if (System.getProperty("os.name").startsWith("Windows")) ".exe" else "").asFile.absolutePath
)
val buildFqpWorker by tasks.registering(Exec::class) {
    group = "verification"
    description = "Builds the Rust worker for Java/Rust Flight integration tests"
    onlyIf { !providers.gradleProperty("fqpWorkerBinary").isPresent }
    workingDir(workerDirectory)
    commandLine("cargo", "build", "--locked")
    inputs.dir(workerDirectory.dir("src"))
    inputs.files(workerDirectory.file("Cargo.toml"), workerDirectory.file("Cargo.lock"))
    outputs.file(workerExecutable)
}
tasks.register<Test>("flightIntegrationTest") {
    group = "verification"
    description = "Sends Java Flight uploads to a real Rust DataFusion worker"
    dependsOn(tasks.named("testClasses"), buildFqpWorker)
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("flightIntegration") }
    systemProperty("fqp.worker.binary", workerExecutable.get())
    inputs.file(workerExecutable)
    // Run process-owning cases serially so Windows can release redirected log handles.
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")
    maxParallelForks = 1
}

val arrowConverter = workerDirectory.file("target/debug/examples/arrow_to_parquet" +
    if (System.getProperty("os.name").startsWith("Windows")) ".exe" else "").asFile
val buildArrowConverter by tasks.registering(Exec::class) {
    group = "verification"
    workingDir(workerDirectory)
    commandLine("cargo", "build", "--locked", "--example", "arrow_to_parquet")
    inputs.dir(workerDirectory.dir("examples"))
    inputs.files(workerDirectory.file("Cargo.toml"), workerDirectory.file("Cargo.lock"))
    outputs.file(arrowConverter)
}
tasks.register<Test>("tpchIntegrationTest") {
    group = "verification"
    description = "Runs Q3/Q5/Q7 across three real DataFusion workers and checks local results"
    dependsOn(tasks.named("testClasses"), buildFqpWorker, buildArrowConverter)
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("tpchIntegration") }
    systemProperty("fqp.worker.binary", workerExecutable.get())
    systemProperty("fqp.arrow.converter", arrowConverter.absolutePath)
    systemProperty("fqp.tpch.output", layout.buildDirectory.dir("live-tpch-q3").get().asFile)
    systemProperty("fqp.tpch.scale", providers.gradleProperty("fqpTpchScale").getOrElse("0.01"))
    inputs.files(workerExecutable, arrowConverter)
    for (query in listOf(3, 5, 7)) {
        outputs.dir(layout.buildDirectory.dir("live-tpch-q$query"))
    }
    // Run process-owning cases serially so Windows can release redirected log handles.
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")
    maxParallelForks = 1
}

tasks.register<JavaExec>("runDataFusionDemo") {
    group = "application"
    description = "Runs the minimal two-worker FQP DataFusion demo"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.calcite.adapter.fqp.demo.FqpDataFusionDemo")
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
