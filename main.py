import pymssql
import random
import uuid
import requests
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional
import os
import firebase_admin
from firebase_admin import credentials, messaging
import json

# =====================================================
# FIREBASE SETUP
# =====================================================
firebase_key = json.loads(os.environ["FIREBASE_KEY"])
cred = credentials.Certificate(firebase_key)
firebase_admin.initialize_app(cred)

# =====================================================
# CONFIG
# =====================================================

# --- Brevo Email Config ---
BREVO_API_KEY = os.getenv("KEY")
BREVO_SENDER_EMAIL = "patelkanostudent@gmail.com"
BREVO_SENDER_NAME = "College ERP System"
BREVO_API_URL = "https://api.brevo.com/v3/smtp/email"

# --- Database Config ---
DB_SERVER = "kano2026.mssql.somee.com:1433"
DB_USER = "Dhvanit_SQLLogin_1"
DB_PASSWORD = os.getenv("PASS")
DB_NAME = "kano2026"

# =====================================================
# FASTAPI APP
# =====================================================
app = FastAPI(title="College ERP Backend")


# =====================================================
# DATABASE CONNECTION
# =====================================================
def get_connection():
    return pymssql.connect(
        server=DB_SERVER,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


# =====================================================
# FCM — FIREBASE PUSH NOTIFICATION
# FIX: Defined BEFORE all endpoints that call it
# =====================================================
def send_fcm(token: str, title: str, body: str) -> None:
    """Send a Firebase push notification. Silently logs errors — never crashes the caller."""
    if not token:
        return
    try:
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            token=token,
        )
        response = messaging.send(message)
        print(f"FCM sent: {response}")
    except Exception as e:
        print(f"FCM error (non-fatal): {e}")


# =====================================================
# BREVO EMAIL — CORE FUNCTION
# =====================================================
def send_email_brevo(to_email: str, subject: str, body: str) -> bool:
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    payload = {
        "sender": {"name": BREVO_SENDER_NAME, "email": BREVO_SENDER_EMAIL},
        "to": [{"email": to_email, "name": to_email}],
        "subject": subject,
        "textContent": body
    }
    try:
        response = requests.post(BREVO_API_URL, json=payload, headers=headers, timeout=30)
        print(f"Brevo Status: {response.status_code} | Response: {response.text}")
        return response.status_code in (200, 201)
    except requests.exceptions.Timeout:
        print("Brevo Error: Request timed out")
        return False
    except requests.exceptions.ConnectionError:
        print("Brevo Error: Cannot connect to Brevo API")
        return False
    except Exception as e:
        print(f"Brevo Error: {e}")
        return False


# =====================================================
# OTP EMAIL
# =====================================================
def generate_otp() -> str:
    return str(random.randint(100000, 999999))


def send_otp_email(email: str, otp: str) -> bool:
    subject = "College Leave Management System — Login OTP"
    body = f"Your OTP is {otp}. Valid for 5 minutes."
    return send_email_brevo(email, subject, body)


# =====================================================
# PARENT NOTIFICATION EMAIL
# =====================================================
def send_parent_email(
    parent_email: str,
    student_name: str,
    from_date: str,
    to_date: str,
    reason: str,
    status: str
) -> bool:
    subject = f"Leave {status} Notification"
    body = (
        f"Dear Parent,\n\n"
        f"Your ward {student_name} applied for leave.\n\n"
        f"From: {from_date}\n"
        f"To: {to_date}\n"
        f"Reason: {reason}\n\n"
        f"Status: {status}\n\n"
        f"Thank you.\n"
        f"College ERP System"
    )
    return send_email_brevo(parent_email, subject, body)


# =====================================================
# PYDANTIC MODELS
# =====================================================
class StudentRegister(BaseModel):
    fullname: str
    roll_no: int
    registration_no: str
    semester: int
    student_email: str
    parent_email: str


class SendOTP(BaseModel):
    email: str


class OTPVerify(BaseModel):
    email: str
    otp: str


class LeaveApply(BaseModel):
    student_id: int
    from_date: str
    to_date: str
    reason: str


class EmergencyLeave(BaseModel):
    student_id: int
    from_date: str
    to_date: str
    reason: str


class Action(BaseModel):
    leave_id: int
    action: str


class ProfessorCreate(BaseModel):
    full_name: str
    email: str


class SaveToken(BaseModel):
    user_id: int
    fcm_token: str


# =====================================================
# TOKEN VALIDATION
# =====================================================
def get_user_from_token(authorization: Optional[str]) -> int:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    # FIX: safe strip — handles "Bearer <token>" or raw token
    token = authorization.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()

    if not token:
        raise HTTPException(status_code=401, detail="Token is empty")

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT UserId FROM LoginSessions WHERE Token=%s AND IsActive=1",
            (token,)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid or expired session")
        return row[0]
    finally:
        conn.close()


# =====================================================
# TEST EMAIL ENDPOINT
# =====================================================
@app.get("/test/email/{to_email}")
def test_email(to_email: str):
    success = send_email_brevo(
        to_email=to_email,
        subject="Test Email from College ERP",
        body="If you received this, Brevo is working correctly!"
    )
    if success:
        return {"status": "success", "message": f"Test email sent to {to_email}"}
    raise HTTPException(status_code=500, detail="Brevo email failed. Check console logs.")


# =====================================================
# DASHBOARD
# =====================================================
@app.get("/dashboard")
def dashboard():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Users WHERE Role='STUDENT' AND IsActive=1")
        count = cursor.fetchone()[0]
        return {"total_students": count}
    finally:
        conn.close()


# =====================================================
# STUDENT REGISTER
# =====================================================
@app.post("/student/register")
def student_register(data: StudentRegister):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM Users WHERE Email=%s", (data.student_email,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")

        cursor.execute(
            "SELECT 1 FROM StudentProfile WHERE RegistrationNo=%s",
            (data.registration_no,)
        )
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Student already registered")

        cursor.execute(
            "INSERT INTO Users (FullName, Email, Role) VALUES (%s, %s, 'STUDENT')",
            (data.fullname, data.student_email)
        )
        cursor.execute("SELECT SCOPE_IDENTITY()")
        student_id = int(cursor.fetchone()[0])

        cursor.execute(
            """
            INSERT INTO StudentProfile
            (StudentId, RollNo, RegistrationNo, Semester, StudentEmail, ParentEmail)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (student_id, data.roll_no, data.registration_no,
             data.semester, data.student_email, data.parent_email)
        )

        cursor.execute(
            """
            SELECT p.ProfessorId
            FROM ProfessorProfile p
            LEFT JOIN StudentProfessorMapping sp
                ON p.ProfessorId = sp.ProfessorId AND sp.Semester = %s
            GROUP BY p.ProfessorId
            HAVING COUNT(sp.StudentId) < 7
            ORDER BY COUNT(sp.StudentId) ASC
            """,
            (data.semester,)
        )
        prof = cursor.fetchone()
        if not prof:
            raise HTTPException(status_code=400, detail="No professor available for this semester")

        cursor.execute(
            "INSERT INTO StudentProfessorMapping (StudentId, ProfessorId, Semester) VALUES (%s, %s, %s)",
            (student_id, prof[0], data.semester)
        )

        conn.commit()
        return {
            "message": "Student registered successfully",
            "professor_id": prof[0],
            "student_id": student_id
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# AUTH — SEND OTP
# =====================================================
@app.post("/auth/send-otp")
def send_otp(data: SendOTP):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT UserId FROM Users WHERE Email=%s AND IsActive=1",
            (data.email,)
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="User not found")

        otp = generate_otp()
        expiry = datetime.now() + timedelta(minutes=5)

        cursor.execute(
            "INSERT INTO EmailOTP (Email, OTPCode, ExpiryTime) VALUES (%s, %s, %s)",
            (data.email, otp, expiry)
        )
        conn.commit()

        email_sent = send_otp_email(data.email, otp)

        if email_sent:
            return {"message": "OTP sent successfully"}
        else:
            # FIX: Return debug OTP only in dev — remove in strict production
            return {
                "message": "OTP saved but email failed",
                "otp_for_debug": otp,
                "warning": "Check Brevo API key and sender verification"
            }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# AUTH — VERIFY OTP
# =====================================================
@app.post("/auth/verify-otp")
def verify_otp(data: OTPVerify):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT OTPId FROM EmailOTP
            WHERE Email=%s AND OTPCode=%s AND IsUsed=0 AND ExpiryTime >= GETDATE()
            """,
            (data.email, data.otp)
        )
        otp_row = cursor.fetchone()
        if not otp_row:
            raise HTTPException(status_code=400, detail="Invalid or expired OTP")

        cursor.execute(
            "UPDATE EmailOTP SET IsUsed=1 WHERE OTPId=%s",
            (otp_row[0],)
        )

        cursor.execute(
            "SELECT UserId, Role FROM Users WHERE Email=%s AND IsActive=1",
            (data.email,)
        )
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        token = str(uuid.uuid4())

        cursor.execute(
            "UPDATE LoginSessions SET IsActive=0 WHERE UserId=%s",
            (user[0],)
        )
        cursor.execute(
            "INSERT INTO LoginSessions (UserId, Token) VALUES (%s, %s)",
            (user[0], token)
        )

        conn.commit()
        return {
            "login": "success",
            "token": token,
            "user_id": user[0],
            "role": user[1]
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# APPLY LEAVE
# FIX: Added conn.commit(), moved FCM inside try block, added return statement
# =====================================================
@app.post("/leave/apply")
def apply_leave(
    data: LeaveApply,
    authorization: str = Header(None)
):
    data.student_id = get_user_from_token(authorization)
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT ProfessorId FROM StudentProfessorMapping WHERE StudentId=%s",
            (data.student_id,)
        )
        prof = cursor.fetchone()

        cursor.execute("SELECT DeanId FROM DeanProfile")
        dean = cursor.fetchone()

        if not prof or not dean:
            raise HTTPException(status_code=400, detail="Professor or Dean configuration missing")

        cursor.execute(
            """
            INSERT INTO LeaveApplications
            (StudentId, ProfessorId, DeanId, ProfessorStatus, DeanStatus, FromDate, ToDate, Reason)
            VALUES (%s, %s, %s, 'PENDING', 'PENDING', %s, %s, %s)
            """,
            (data.student_id, prof[0], dean[0], data.from_date, data.to_date, data.reason)
        )

        # FIX: commit BEFORE FCM, so DB is saved even if FCM fails
        conn.commit()

        # FIX: FCM inside try block, connection still open
        cursor.execute(
            "SELECT p.FcmToken FROM Users p "
            "JOIN StudentProfessorMapping sp ON p.UserId = sp.ProfessorId "
            "WHERE sp.StudentId = %s",
            (data.student_id,)
        )
        prof_token_row = cursor.fetchone()
        if prof_token_row and prof_token_row[0]:
            send_fcm(
                prof_token_row[0],
                "New Leave Request",
                f"Student {data.student_id} has applied for leave."
            )

        return {"message": "Leave application submitted successfully"}  # FIX: was missing

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# EMERGENCY LEAVE
# FIX: Added conn.commit(), moved FCM inside try, added return statement
# =====================================================
@app.post("/leave/emergency")
def emergency_leave(
    data: EmergencyLeave,
    authorization: str = Header(None)
):
    data.student_id = get_user_from_token(authorization)
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT ProfessorId FROM StudentProfessorMapping WHERE StudentId=%s",
            (data.student_id,)
        )
        prof = cursor.fetchone()

        cursor.execute("SELECT DeanId FROM DeanProfile")
        dean = cursor.fetchone()

        if not prof or not dean:
            raise HTTPException(status_code=400, detail="Configuration error")

        cursor.execute(
            """
            INSERT INTO LeaveApplications
            (StudentId, ProfessorId, DeanId, ProfessorStatus, DeanStatus, FromDate, ToDate, Reason)
            VALUES (%s, %s, %s, 'SKIPPED', 'PENDING', %s, %s, %s)
            """,
            (data.student_id, prof[0], dean[0], data.from_date, data.to_date, data.reason)
        )

        # FIX: commit before FCM
        conn.commit()

        # FIX: FCM inside try block, connection still open
        cursor.execute("SELECT FcmToken FROM Users WHERE Role='DEAN'")
        dean_token_row = cursor.fetchone()
        if dean_token_row and dean_token_row[0]:
            send_fcm(
                dean_token_row[0],
                "EMERGENCY LEAVE",
                f"Student {data.student_id} requested emergency leave."
            )

        return {"message": "Emergency leave submitted successfully"}  # FIX: was missing

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# STUDENT LEAVE VIEWS
# =====================================================
@app.get("/student/leaves/{student_id}")
def student_leaves(student_id: int):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT TOP 5
                LeaveId, ProfessorStatus, DeanStatus, FinalStatus, FromDate, ToDate, Reason
            FROM LeaveApplications
            WHERE StudentId=%s
            ORDER BY LeaveId DESC
            """,
            (student_id,)
        )
        rows = cursor.fetchall()

        results = []
        for r in rows:
            prof_status = r[1] or ""
            dean_status = r[2] or ""
            # FIX: FinalStatus can be NULL — use empty string fallback
            final_status = r[3] or ""

            if prof_status == "PENDING" and dean_status == "PENDING":
                display_status = "Waiting for Professor"
            elif prof_status == "APPROVED" and dean_status == "PENDING":
                display_status = "Waiting for Dean"
            elif prof_status == "SKIPPED" and dean_status == "PENDING":
                display_status = "Waiting for Dean (Emergency)"
            elif final_status == "FINAL_APPROVED":
                display_status = "Approved"
            elif "REJECTED" in final_status:
                display_status = "Rejected"
            else:
                display_status = final_status or "Processing"

            results.append({
                "leave_id": r[0],
                "professor_status": prof_status,
                "dean_status": dean_status,
                "final_status": final_status,
                "display_status": display_status,
                "from_date": str(r[4]),
                "to_date": str(r[5]),
                "reason": r[6]
            })
        return results
    finally:
        conn.close()


@app.get("/student/rejected/{student_id}")
def student_rejected(student_id: int):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT LeaveId, FromDate, ToDate, Reason
            FROM LeaveApplications
            WHERE StudentId=%s AND FinalStatus LIKE 'REJECTED%'
            """,
            (student_id,)
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/student/approved/{student_id}")
def student_approved(student_id: int):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT LeaveId, FromDate, ToDate, Reason
            FROM LeaveApplications
            WHERE StudentId=%s AND FinalStatus='FINAL_APPROVED'
            """,
            (student_id,)
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


# =====================================================
# DEAN APIs
# =====================================================
@app.get("/dean/pending")
def dean_pending():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT l.LeaveId, l.StudentId, u.FullName,
                   s.Semester, l.FromDate, l.ToDate, l.Reason
            FROM LeaveApplications l
            JOIN Users u ON l.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = l.StudentId
            WHERE l.DeanStatus='PENDING' AND l.ProfessorStatus != 'PENDING'
            """
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/dean/approved")
def dean_approved():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT l.LeaveId, l.StudentId, u.FullName,
                   s.Semester, l.FromDate, l.ToDate, l.Reason
            FROM LeaveApplications l
            JOIN Users u ON l.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = l.StudentId
            WHERE l.DeanStatus='APPROVED'
            """
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/dean/emergency")
def dean_emergency():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT l.LeaveId, l.StudentId, u.FullName,
                   s.Semester, l.FromDate, l.ToDate, l.Reason
            FROM LeaveApplications l
            JOIN Users u ON l.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = l.StudentId
            WHERE l.ProfessorStatus='SKIPPED' AND l.DeanStatus='PENDING'
            """
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/dean/students")
def dean_students():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT u.UserId, u.FullName, s.RollNo,
                   s.RegistrationNo, s.Semester,
                   s.StudentEmail, s.ParentEmail
            FROM Users u
            JOIN StudentProfile s ON u.UserId = s.StudentId
            WHERE u.Role='STUDENT' AND u.IsActive=1
            """
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/dean/semester-wise")
def semester_wise():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT s.Semester, u.FullName AS StudentName,
                   s.RollNo, pUser.FullName AS ProfessorName
            FROM StudentProfessorMapping sp
            JOIN Users u ON sp.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = u.UserId
            JOIN Users pUser ON pUser.UserId = sp.ProfessorId
            ORDER BY s.Semester, s.RollNo
            """
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.post("/dean/add-professor")
def add_professor(prof: ProfessorCreate):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT UserId FROM Users WHERE Email=%s", (prof.email,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email already exists")

        cursor.execute(
            "INSERT INTO Users (FullName, Email, Role, IsActive, CreatedAt) VALUES (%s,%s,'PROFESSOR',1,GETDATE())",
            (prof.full_name, prof.email)
        )
        cursor.execute("SELECT SCOPE_IDENTITY()")
        professor_id = int(cursor.fetchone()[0])

        professor_code = f"PROF{professor_id:04d}"

        cursor.execute(
            "INSERT INTO ProfessorProfile (ProfessorId, ProfessorCode, Email) VALUES (%s,%s,%s)",
            (professor_id, professor_code, prof.email)
        )

        conn.commit()
        return {
            "message": "Professor added successfully",
            "professor_id": professor_id,
            "professor_code": professor_code
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# DEAN ACTION ON LEAVE
# FIX: Removed duplicate FCM send, fixed cursor-after-commit issue
# =====================================================
@app.post("/leave/dean-action")
def dean_action(data: Action):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        status = "APPROVED" if data.action == "APPROVED" else "REJECTED"
        final_status = "FINAL_APPROVED" if status == "APPROVED" else "REJECTED_BY_DEAN"

        cursor.execute(
            """
            UPDATE LeaveApplications
            SET DeanStatus=%s, FinalStatus=%s
            WHERE LeaveId=%s
            """,
            (status, final_status, data.leave_id)
        )

        # Fetch all needed data BEFORE commit
        cursor.execute(
            """
            SELECT u.FullName, s.ParentEmail, l.FromDate, l.ToDate, l.Reason, u.FcmToken
            FROM LeaveApplications l
            JOIN Users u ON l.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = l.StudentId
            WHERE l.LeaveId=%s
            """,
            (data.leave_id,)
        )
        row = cursor.fetchone()

        conn.commit()  # Commit once, cleanly

        # Send parent email (after commit — non-fatal if fails)
        email_sent = False
        if row:
            try:
                email_sent = send_parent_email(
                    parent_email=row[1],
                    student_name=row[0],
                    from_date=str(row[2]),
                    to_date=str(row[3]),
                    reason=row[4],
                    status=status
                )
            except Exception as mail_err:
                print(f"Parent email error (non-fatal): {mail_err}")

            # FIX: Single FCM send (was sending twice before)
            student_fcm_token = row[5]
            if student_fcm_token:
                send_fcm(
                    student_fcm_token,
                    "Leave Decision",
                    f"Your leave has been {status.lower()} by the Dean."
                )

        return {
            "message": f"Dean action recorded: {status}",
            "parent_email_sent": email_sent
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# PROFESSOR APIs
# =====================================================
@app.get("/professor/pending/{professor_id}")
def professor_pending(professor_id: int):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT l.LeaveId, l.StudentId, u.FullName,
                   s.Semester, l.FromDate, l.ToDate, l.Reason
            FROM LeaveApplications l
            JOIN Users u ON l.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = l.StudentId
            WHERE l.ProfessorId=%s AND l.ProfessorStatus='PENDING'
            """,
            (professor_id,)
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/professor/approved/{professor_id}")
def professor_approved(professor_id: int):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT l.LeaveId, u.FullName, s.Semester, l.FromDate, l.ToDate
            FROM LeaveApplications l
            JOIN Users u ON l.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = l.StudentId
            WHERE l.ProfessorId=%s AND l.ProfessorStatus='APPROVED'
            """,
            (professor_id,)
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/professor/rejected/{professor_id}")
def professor_rejected(professor_id: int):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT l.LeaveId, u.FullName, s.Semester, l.FromDate, l.ToDate
            FROM LeaveApplications l
            JOIN Users u ON l.StudentId = u.UserId
            JOIN StudentProfile s ON s.StudentId = l.StudentId
            WHERE l.ProfessorId=%s AND l.ProfessorStatus='REJECTED'
            """,
            (professor_id,)
        )
        rows = cursor.fetchall()
        return [list(r) for r in rows]
    finally:
        conn.close()


@app.get("/professor/students/{professor_id}")
def professor_students(professor_id: int):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sp.Semester, COUNT(*) AS total_students
            FROM StudentProfessorMapping sp
            WHERE sp.ProfessorId=%s
            GROUP BY sp.Semester
            """,
            (professor_id,)
        )
        rows = cursor.fetchall()
        return [{"semester": r[0], "total_students": r[1]} for r in rows]
    finally:
        conn.close()


# =====================================================
# PROFESSOR ACTION ON LEAVE
# FIX: Added missing conn.commit() — without this, DB was never updated
# =====================================================
@app.post("/leave/professor-action")
def professor_action(data: Action):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        status = "APPROVED" if data.action == "APPROVED" else "REJECTED"
        final_status = (
            "FORWARDED_TO_DEAN" if status == "APPROVED"
            else "REJECTED_BY_PROFESSOR"
        )

        cursor.execute(
            """
            UPDATE LeaveApplications
            SET ProfessorStatus=%s, FinalStatus=%s
            WHERE LeaveId=%s
            """,
            (status, final_status, data.leave_id)
        )

        # Fetch Dean FCM token before commit
        dean_fcm_token = None
        if data.action == "APPROVED":
            cursor.execute("SELECT FcmToken FROM Users WHERE Role='DEAN'")
            dean_row = cursor.fetchone()
            if dean_row:
                dean_fcm_token = dean_row[0]

        conn.commit()  # FIX: This was completely missing before

        # Send FCM after commit
        if dean_fcm_token:
            send_fcm(
                dean_fcm_token,
                "Leave Forwarded",
                f"Professor approved Leave ID {data.leave_id}. Your action is required."
            )

        return {"message": f"Professor action recorded: {status}"}

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =====================================================
# USER PROFILE
# =====================================================
@app.get("/user/profile")
def get_profile(authorization: str = Header(None)):
    user_id = get_user_from_token(authorization)

    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT FullName, Email, Role FROM Users WHERE UserId=%s",
            (user_id,)
        )
        user = cursor.fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        role = user[2]
        profile_data = {
            "user_id": user_id,
            "name": user[0],
            "email": user[1],
            "role": role
        }

        if role == "STUDENT":
            cursor.execute(
                """
                SELECT RollNo, RegistrationNo, Semester, StudentEmail, ParentEmail
                FROM StudentProfile WHERE StudentId=%s
                """,
                (user_id,)
            )
            d = cursor.fetchone()
            if d:
                profile_data.update({
                    "roll_no": d[0],
                    "registration_no": d[1],
                    "semester": d[2],
                    "student_email": d[3],
                    "parent_email": d[4],
                })

        elif role == "PROFESSOR":
            cursor.execute(
                "SELECT ProfessorCode, Email FROM ProfessorProfile WHERE ProfessorId=%s",
                (user_id,)
            )
            d = cursor.fetchone()
            if d:
                profile_data.update({"professor_code": d[0], "email": d[1]})

        elif role == "DEAN":
            cursor.execute(
                "SELECT DeanCode, Email FROM DeanProfile WHERE DeanId=%s",
                (user_id,)
            )
            d = cursor.fetchone()
            if d:
                profile_data.update({"dean_code": d[0], "email": d[1]})

        return profile_data

    finally:
        conn.close()


# =====================================================
# SAVE FCM TOKEN
# =====================================================
@app.post("/save-fcm-token")
def save_fcm_token(
    data: SaveToken,
    authorization: str = Header(None)
):
    user_id_from_token = get_user_from_token(authorization)

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE Users SET FcmToken=%s WHERE UserId=%s",
            (data.fcm_token, user_id_from_token)
        )
        conn.commit()
        return {"message": "FCM token saved successfully"}
    except Exception as e:
        conn.rollback()
        print(f"FCM token save error: {e}")
        raise HTTPException(status_code=500, detail="Database error saving FCM token")
    finally:
        conn.close()


# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
