async function runPipeline(){

const res = await fetch("/run");

const data = await res.json();

alert("Pipeline started\nTopic: " + data.topic);

}

async function testRender(){

const res = await fetch("/run");

const data = await res.json();

console.log(data);

alert("Render triggered");

}
