from configupdater import ConfigUpdater

def reset_placeholders(config_path="dwh.cfg"):
    """
    Force replace HOST and IAM_ROLE_ARN lines with dynamic placeholders via text replacement.
    This avoids issues with ConfigUpdater removing comments or formatting.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    with open(config_path, "w", encoding="utf-8") as f:
        for line in lines:
            if line.strip().startswith("HOST="):
                f.write("HOST=${redshift_host}\n")
            elif line.strip().startswith("IAM_ROLE_ARN="):
                f.write("IAM_ROLE_ARN=${iam_role_arn}\n")
            else:
                f.write(line)

    print("🔄 Placeholders in dwh.cfg have been reset via text replacement.")