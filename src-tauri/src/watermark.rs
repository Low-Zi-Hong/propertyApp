use image::{ImageFormat, Rgba};
use ab_glyph::{FontRef, PxScale,Font};
use imageproc::drawing::draw_text_mut;
use std::path::Path;
use std::fs;

use crate::WATERMARKTEXT;

#[tauri::command]
// ✨ 注意：现在多接收了一个参数 watermark_text
pub async fn add_watermark(image_paths: Vec<String>) -> Result<String, String> {
    if image_paths.is_empty() {
        return Err("No images to process".to_string());
    }

    let font_data = include_bytes!("../assets/font.ttf");
    let font = FontRef::try_from_slice(font_data)
        .map_err(|e| format!("Error constructing Font: {}", e))?;

    // 2. 设定字体大小 (之后你可以把这个也做成前端传过来的参数)
    let height = 60.0;
    let scale = PxScale::from(height);

    // 3. 设定颜色：白色字，黑色阴影
    let text_color = Rgba([255, 255, 255, 230]); // 白色，稍微带点透明
    let shadow_color = Rgba([0, 0, 0, 150]);     // 黑色半透明阴影

    // 5. 遍历图片画字
    for path_str in image_paths {
        let img_path = Path::new(&path_str);
        let mut base_image = image::open(img_path)
            .map_err(|e| format!("无法读取图片 {}: {}", path_str, e))?;

        // 算出图片的中心点 (近似值，假设每行字大概这么宽)
        let img_width = base_image.width() as i32;
        let img_height = base_image.height() as i32;
        
        // 把传过来的文字按换行符拆开
        let watermark_string = WATERMARKTEXT.to_string();
        let lines: Vec<&str> = watermark_string.lines().collect();
        
        // 计算起始 Y 坐标，让一堆文字整体居中
        let total_text_height = (lines.len() as f32 * height * 1.2) as i32;
        let mut current_y = (img_height - total_text_height) / 2;

        for line in lines {
            // 简单估算文字宽度居中 (一个字符大概是高度的0.5倍宽)
            let estimated_width = (line.len() as f32 * (height * 0.5)) as i32;
            let current_x = (img_width - estimated_width) / 2;

            // 魔法：先往右下方偏移 3 个像素，画黑色的阴影
            draw_text_mut(&mut base_image, shadow_color, current_x + 3, current_y + 3, scale, &font, line);
            
            // 再在正中心画白色的字，这样字就立体了，在白墙上也能看清！
            draw_text_mut(&mut base_image, text_color, current_x, current_y, scale, &font, line);

            // 往下走一行
            current_y += (height * 1.2) as i32; 
        }

        let file_name = img_path.file_name().unwrap();
        
        // 强制存为高质量 JPEG (RGBA -> RGB 的转换通常在底层自动处理，如果不报错就没问题)
        let rgb_image = image::DynamicImage::ImageRgb8(base_image.into_rgb8());
        rgb_image.save_with_format(img_path, image::ImageFormat::Jpeg)
            .map_err(|e| e.to_string())?;
    }

    Ok("动态水印批量添加成功！".to_string())
}