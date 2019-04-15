/**
	* Модуль загружающий в БД (mariadb) статистику по крипте 
	* Источник - api.coincap.io
	*
	* Структура таблицы БД:
	* 	id INT, open FLOAT(18,16), high FLOAT(18,16), low FLOAT(18,16), 
	*	close FLOAT(18,16), period BIGINT, data DATETIME
	*
	* Необходимое модули: 
	* 	fs, mariadb,axios
	*
	* Предопределенные костанты:
	*	user, password, database - подключение к бд
	*	change - цены в bitcoin
	*
	* Входные данные:
	* 	timeStart, timeEnd, step - в миллисекундах
	*	interval - рассматриваемы свечи ("m15","m5"...)
	*	base - рассматриваемые данные
	*	 
	*/

export function parserToMariadb(timeStart, timeEnd, step, base, interval ){
	
	const minute = 60000;

	var timeStart = timeStart || 1514764800000; // Mon, 01 Jan 2018 00:00:00 GMT
	var timeEnd   = timeEnd   || 1546300800000; // Mon, 01 Jan 2019 00:00:00 GMT
	var step      = step      || minute * 60 *24 *6;
	var interval  = interval  || "m15";
	
	var base      = "tron";
	
	var change    = "bitcoin";
	let d = new Date(timeStart); console.log(d.toString());
	var time      = timeStart - step;
	d.setTime(time+step);
	console.log(d.toString());

	const file = require("fs");
	const mariadb = require('mariadb');
	const pool = mariadb.createPool({ 
	     user:'bot', 
	     password: ' ',
	     database: 'crypt'
	});

	function query(info,base,interval){
		return new Promise(async function(resolve, reject){
		  let conn;
		  try {
			conn = await pool.getConnection();
			let d = new Date(info.period);
			let query = "INSERT INTO " + base + "_" + interval +
				" value (" + index + "," + info.open + "," + info.high + "," + 
				info.low +","+ info.close + "," + info.period + ",'" +
				d.getFullYear() +"-"+ parseInt( d.getMonth() + 1 ) + "-" + d.getDate() + " " +
				d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds() + "')";
			
			const res = await conn.query(query, [1, "mariadb"]);
			console.log(query);

			resolve(res);

		  } catch (err) {
			reject(err);
		  } finally {
			if (conn) return conn.end();
		  }
		});
	}

	function insertArray(data,callback,base,interval){

		let loop = function(i){
			console.log(i,data.length);
			if(i<data.length){
				query(data[i],base,interval)
				.then(
					response => {
						console.log("ok");
						loop(i+1);
						index++;
					},
					err => {
						console.log("error");
					}
				);
			} else {
				callback();
			}
		}
		
		loop(0);	
	}

	function parseCoin(interval,base,change,timeStart,timeEnd,callback){
		var axios = require('axios')
		console.log('https://api.coincap.io/v2/candles?exchange=binance&interval='+
		    	interval+'&baseId='+base+'&quoteId='+change+
		    	'&start='+timeStart+'&end='+timeEnd);
		axios
		    .get('https://api.coincap.io/v2/candles?exchange=binance&interval='+
		    	interval+'&baseId='+base+'&quoteId='+change+
		    	'&start='+timeStart+'&end='+timeEnd)
		    .then(
		      value => {
		        info = value.data.data;
		        insertArray(info,callback,base,interval);
		                
		      },reason => {
		      	dt=new Date();
		      	console.log('Ошибка парсера'+'\n'+reason+'\n');
		      	file.appendFileSync("errorParse.txt",dt.toString()+'\n'+'Ошибка парсера'+'\n'+reason+'\n');
		      }
		    );
	}

	var index=0;

	function loop(){
		time = time + step;

		if( (time + step) < timeEnd ){
			console.log("итерация",step);
			let dt = new Date(time);
			console.log(dt.toString());
			dt.setTime(time+step);
			console.log(dt.toString());
			parseCoin(interval, base, change, time, time+step, loop);
		}
		else
			console.log('parse end');
	}
	loop();
}