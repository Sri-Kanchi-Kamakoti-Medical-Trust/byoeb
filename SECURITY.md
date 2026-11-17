# Security Policy – CataractBot (Sankara Eye Hospital)

CataractBot is operated by Sankara Eye Hospital and handles patient health information.
Security of this deployment is owned by Sankara, not by Microsoft or the upstream BYOeB project.

## Reporting a vulnerability

**Please do not report security problems through public GitHub issues, pull requests or discussions.**

Email **saravanan@sankaraeye.com** (Saravanan D, Chief Technology Officer, Information Systems)
with the subject line `CataractBot security report`.

Please include, as far as you can:

* what the problem is and what an attacker could do with it
* the affected endpoint, file or commit
* steps to reproduce (please use test data only; never access or copy real patient data)

We acknowledge reports within **3 working days** and keep the reporter informed until the issue is fixed.
Please give us a reasonable time to fix the problem before telling anyone else about it.

## Personal data breaches

If a report shows that patient data may have been exposed, the security owner above starts the
incident-response plan, including notifying the Data Protection Board of India and the affected
patients as required by the Digital Personal Data Protection Act, 2023.

## Scope

* The CataractBot web app, its scheduled jobs and its Azure resources (resource group `Cataract_Bot`)
* This repository

## Credential rotation

Keys and passwords are rotated every quarter: **1 February, 1 May, 1 August and 1 November**.
Secrets are kept only in Azure Key Vault `catbot-kv`; never commit them to this repository
(every push is scanned for secrets).
