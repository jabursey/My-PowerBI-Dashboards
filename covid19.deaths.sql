--Reported Deaths
SELECT 
       Patient_ID
      ,Age
      ,Gender
      ,Region
      ,Preexisting_Condition
      ,Date_of_Infection
      ,COVID_Strain
      ,Symptoms
      ,Severity
      ,Hospitalized
      ,Hospital_Admission_Date
      ,Hospital_Discharge_Date
      ,ICU_Admission
      ,Ventilator_Support
      ,Recovered
      ,Date_of_Recovery
      ,Reinfection
      ,Date_of_Reinfection
      ,Vaccination_Status
      ,Vaccine_Type
      ,Doses_Received
      ,Date_of_Last_Dose
      ,Long_COVID_Symptoms
      ,Occupation
      ,Smoking_Status
      ,BMI
      ,CASE WHEN Date_of_Reinfection IS NULL THEN Date_of_Infection ELSE Date_of_Reinfection END AS Death_Date --Death Date was created from logic of paitient getting infected and/or reinfected
FROM covid19
WHERE Recovered = 'No';