const fs = require('fs');
let data = require('./csvjson.json');
let inputData = new Map();

for(let counter = 0; counter < data.length; counter++) {
	let key = data[counter].A_id;
	if(!inputData.get(key) && (data[counter].B_id != null || data[counter].B_id != undefined || data[counter].B_id != '')) {
		inputData.set(key, [])
	}
	if(data[counter].B_id != null || data[counter].B_id != undefined || data[counter].B_id != '') {
		inputData.get(key).push(data[counter].B_id);
	}
}

let preprocessData = new Promise(function(resolve, reject) {
	for(let [key, values] of inputData.entries()) {
		inputData.get(key).forEach(function(value) {
			if(!inputData.get(value)) {
				let index = inputData.get(key).indexOf(value);
				delete inputData.get(key)[index];
			}	
		});
		let filtered = inputData.get(key).filter(function() {
			return true;
		})
		inputData.set(key, filtered);
	}	
	console.log("Preprocessed");
	resolve(inputData);
})

let writeToFile = new Promise(function(resolve, reject) {
	preprocessData.then(function(inputData) {
		for(let [key, values]  of inputData.entries()) {
			fs.appendFileSync('input.txt', key + ' ' + values.join(','));
			fs.appendFileSync('input.txt', '\n');
		}
		console.log("Written to file");	
		resolve('Done');
	})
});

writeToFile.then(function(message) {
	console.log(message);
})