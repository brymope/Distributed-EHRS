<h1>Distributed Electronic Health Record System</h1>

This system of 3 Hospitals has 3 microservices each, the I dentity & Access Management (IAM) and a Client which makes a total of 11 console to run. Due to this, the deployment is setup for Docker.


<h2>Deployment with Docker on a local computer</h2>
<ul>
  <li>Install Docker Desktop<br/></li>
  <li>Open PowerShell or Command Prompt and navigate to the project directory</li>
  <li>At the project directory, type <code>docker compose up -d</code> and wait until the build completes. If build stops or hangs while building, Press <code>CTRL + C </code> to stop the process, then type <code>docker compose down -v </code> then build again using <code>docker compose up -d</code></li>
  <li>After the build is complete, Open the "installed Docker Desktop"</li>
  <li>Check your container and start the services if it is not running</li>
</ul> 


<h2>Testing the System</h2>
<ul>
  <li>Open Powershell or command prompt to use as a Client</li>
  <li>Use CURL to perform CRUD operations</li>
</ul>

Local addresses of the Microservices

IAM service
<ul>
  <li><code>http://localhost:7000</code></li>
</ul>
  
Hospital patient microservice URLs
<ul>
  <li>HOSPITAL 1 <code>http://localhost:5001</code></li>
  <li>HOSPITAL 2 <code>http://localhost:5002</code></li>
  <li>HOSPITAL 3 <code>http://localhost:5003</code></li>
</ul>


Audit microservice URLs
<ul>
  <li><code>http://localhost:6001</code></li>
  <li><code>http://localhost:6002</code></li>
  <li><code>http://localhost:6003</code></li>
</ul>


<strong>Risk Analysis microservice URLs</strong>
<ul>
  <li>Hospital 1 <code>http://localhost:7001</code></li>
  <li>Hospital 2 <code>http://localhost:7002</code></li>
  <li>Hospital 3 <code>http://localhost:7003</code></li>
</ul>

<table>
  <thead>
    <tr>
      <th>Endpoint</th><th>Method</th><th>Description</th>
    </tr>
  </thead>
  <tr>
    <td><code>/status</code></td><td>GET</td> <td>Check the health status of an hospital's patient microservice</td>
  </tr>
  <tr>
    <td><code>http://localhost:7000/login</code></td> <td>POST</td> <td> Login to get a JWT Token for authentication and authoraization</td>
  </tr>
    <tr>
    <td><code>/patient</code></td> <td>POST</td> <td> Create a new patient record</td>
  </tr>
    <tr>
    <td><code>/patients</code></td> <td>GET</td> <td> View a full list of patients and hospital visits</td>
  </tr>
    <tr>
    <td><code>/patient/string:PID</code></td> <td>GET</td> <td> View the full information about a patient and hospital visits</td>
  </tr>
    <tr>
    <td><code>/risk/string:PID</code></td> <td>GET</td> <td> Check a Patient's probability of hospital readmission within the next 30days</td>
  </tr>
    <tr>
    <td><code>/patient/string:PID/visit</code></td> <td>POST</td> <td> Add a Patient's hospital visit</td>
  </tr>
    <tr>
    <td><code>/audit</code></td> <td>POST</td> <td> Check the Audit Log</td>
  </tr>
    <tr>
    <td><code>/patient/string:PID</code></td> <td>PUT </td> <td> Update patient's information</td>
  </tr>
    <tr>
    <td><code>/patient/string:PID</code></td> <td>DELETE</td> <td> Deelete Patient's data</td>
  </tr>
</table>
