-- Youth Survey(VI-SPDAT TAY v1.0) IDs: 55f21054-36c4-4939-a63c-de01dc4dfee9 , cbfa78b5-6bb3-4af0-a301-a7b2c3fdd209
-- Family Survey(VI-SPDAT Family 2.0) IDs:  fec88521-d60c-40bc-89f4-f390797baba1 , be0e17a5-aeb2-466a-8395-5da1db0fb8a5
-- Singles Survey(VI-SPDAT Single 2.0.1) ID: ec576a06-4b7d-4507-a02b-4025d2177c30 , f07de7b5-c6cd-42c5-985a-f762b01fd720

SELECT * 
FROM crosstab('SELECT submission_id,question_id,response_text FROM survey.response',
			  'SELECT question_id
               FROM survey.response
               WHERE survey_id=''55f21054-36c4-4939-a63c-de01dc4dfee9'' AND (question_id::Text=''5260ea29-4329-4da8-8091-8aa05fd62ff7'' OR question_id::Text=''3fcc8ec3-65f1-4b36-b793-a8eebf4e6e0b'')
               GROUP BY question_id') 
     AS submissions(sid uuid,q1 Text,q2 Text)
WHERE q1 IS NOT NULL


--VALUES (\"d6e2993f-b4cd-412b-bdf7-4229c89eb273\"), (\"4467ba3e-644e-4d6e-a14e-ec126b2b5a3b\")

--SELECT question_id FROM survey.response GROUP BY question_id