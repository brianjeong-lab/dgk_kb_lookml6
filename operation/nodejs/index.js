const Compute = require('@google-cloud/compute');
const compute = new Compute();

getVms = async (callback) => {

   //payload.label='env=dev';
   const options = {filter: `labels.env=dev`};
   const [vms] = await compute.getVMs(options);

   vms.map(async (instance) => {
       console.log(instance.zone.id) ;
       console.log(instance) ;
   })
}

getVms();

if("asia-northeast3-a".indexOf("asia-northeast3")>= 0)
	console.log("True1");

if("asia-northeast3-a".indexOf("1asia-northeast3")>= 0)
	console.log("True");
